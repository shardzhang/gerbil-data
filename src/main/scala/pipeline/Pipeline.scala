package pipeline

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import featurizer.{Featurizer, FieldType}
import utils.LogUtils.green_println
import pipeline.serde.{Vocabulary, SampleWriter}
import pipeline.stats.{DataQualityTracker, PosInfo, RunningValueStats}


/**
 * End-to-end training sample generation pipeline.
 *
 * Orchestrates the full workflow from raw samples to featurized TFRecord/Parquet output:
 *
 *  1. Load and parse raw samples from the input directory
 *     2. Time-based train/val/test split to prevent data leakage
 *     3. Compute target distribution and build target-map (frequency thresholding)
 *     4. Collect hash statistics across all samples, build embedding vocabulary (pos-map)
 *     5. Encode each sample via the Featurizer and write to TFRecord or Parquet
 *     6. Report data quality metrics (parse success rate, target distribution, etc.)
 *
 * Supports incremental training: pos-map and target-map from previous runs
 * can be loaded and updated with new data.
 *
 * @tparam T the training sample type, must have a ClassTag for Spark serialization
 */
abstract class Pipeline[T: ClassTag] extends Serializable {
  @transient var hadoopConf: Configuration = new Configuration()
  /** Persists/restores position-map and target-map across runs. */
  @transient val vocabulary: Vocabulary = new Vocabulary(hadoopConf)
  /** Serializes featurized samples to TFRecord or Parquet. */
  @transient lazy val sampleWriter: SampleWriter[T] = new SampleWriter[T](() => featurizer, max_dim)
  /** Tracks record counts, parse success rates, and target distributions at each ETL stage. */
  @transient lazy val qualityTracker: DataQualityTracker = new DataQualityTracker()

  /** Maximum hash dimension (typically 2^60 for 64-bit embedding space). */
  def max_dim: Long

  /** The featurizer that converts raw samples into encoded features. */
  def featurizer: Featurizer[T]

  /** Loads and parses raw training samples from the given input directory. */
  def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(T, Boolean)]

  /** Extracts the target label (as Int) from a sample for threshold statistics. */
  def parseTarget(sample: T): Int

  /** Extracts the timestamp (millis) from a sample for time-based split. */
  def parseTimestamp(sample: T): Long

  /**
   * Whether to re-encode target values through target_map (sequential indexing).
   * Override to return false when raw target values should be written directly.
   */
  def useTargetMap: Boolean = true

  /** Down-samples negative samples (target == 0) by `sample_ratio`. Positive samples are always kept. */
  def keepSample(sample: T, sample_ratio: Double): Boolean

  /** Computes hash info (field name, index, type, raw, hash, value) for vocabulary building. */
  def getHashInfo(sample: T, encoder: Featurizer[T]): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.getHashInfo(sample, max_dim)
  }

  /** Aligns posDim entries so all features sharing the same (f_index, f_type) use the same dim, used for vocabulary sharing. */
  def alignFieldDim(posDim: HashMap[(String, Int, Int), Int],
                    fieldDim: HashMap[(Int, Int), Int],
                    featurizer: Featurizer[_]): Unit = {
    val nameIndexMap = featurizer.getFieldInfo()
      .groupBy(_._2)
      .map {
        case (idx, entries) => (idx, entries.map(_._1))
      }

    posDim.clear()
    fieldDim.foreach { case ((f_index, f_type), dim) =>
      val f_names = nameIndexMap.getOrElse(f_index, Nil)
      f_names.foreach { f_name =>
        posDim.put((f_name, f_index, f_type), dim)
      }
    }
  }


  /**
   *
   * @param allSamples
   * @param trainRatio
   * @param valRatio
   * @return
   */
  def splitSamples(allSamples: RDD[(T, Boolean)],
                   trainRatio: Double,
                   valRatio: Double
                  ): (RDD[(T, Boolean)], RDD[(T, Boolean)], RDD[(T, Boolean)]) = {

    if (trainRatio >= 1.0) {
      return (allSamples, null, null)
    }

    val sorted: RDD[(T, Boolean)] = allSamples.sortBy {
      case (sample, _) => parseTimestamp(sample)
    }
    val indexed: RDD[((T, Boolean), Long)] = sorted.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
    val total = indexed.count()
    val trainEnd = (total * trainRatio).toLong
    val valEnd = trainEnd + (total * valRatio).toLong

    val train: RDD[(T, Boolean)] = indexed
      .filter { case (_, idx) => idx < trainEnd }.map(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val valid: RDD[(T, Boolean)] = indexed
      .filter { case (_, idx) => idx >= trainEnd && idx < valEnd }.map(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val test: RDD[(T, Boolean)] = indexed
      .filter { case (_, idx) => idx >= valEnd }.map(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val trainCount = train.count()
    val valCount = valid.count()
    val testCount = test.count()

    indexed.unpersist()
    green_println(s"train/val/test split: ${total}=total, train=${trainCount} (${trainRatio * 100}%), val=${valCount} (${valRatio * 100}%), test=${testCount} (${(1.0 - trainRatio - valRatio) * 100}%)")

    // Record split counts to quality tracker
    qualityTracker.recordCounts("train_split", trainCount)
    qualityTracker.recordCounts("val_split", valCount)
    qualityTracker.recordCounts("test_split", testCount)

    (train, valid, test)
  }


  /**
   * Build target-map: assign a sequential index to each target value meeting the threshold
   * Build position-map: assign embedding indices to categorical feature values;
   *
   * @param targetOccurs fixme
   * @param targetThreshold
   * @param targetMap
   */
  def generateVocabulary(trainingSample: RDD[(T, Boolean)],
                         targetThreshold: Int,
                         featureThreshold: Int,
                         targetMap: mutable.HashMap[Int, Int],
                         posMap: mutable.HashMap[(Int, Long), PosInfo],
                         posDim: mutable.HashMap[(String, Int, Int), Int]
                        ): Unit = {
    green_println(s"old targetMap.size: ${targetMap.size}.")

    // Aggregate sample counts per target value
    val targetOccurs: Array[(Int, Int)] = trainingSample
      .filter(r => r._2) // 仅取出有效样本
      .map { case (sample, _) => sample }
      .map(sample => (parseTarget(sample), 1))
      .reduceByKey(_ + _)
      .collect()
      .sortWith((a, b) => a._2 > b._2)

    // data check. Count parse success vs. failure
    val retCounts: Array[(Boolean, Int)] = trainingSample
      .map(s => (s._2, 1))
      .reduceByKey(_ + _)
      .collect()
    val totalCount = retCounts.map(_._2).sum
    val validCount = retCounts.find(_._1).map(_._2.toLong).getOrElse(0L) // count of successfully parsed samples
    qualityTracker.record("train_stats", totalCount, validCount, targetOccurs.toSeq)

    //====================== part1: target position vocabulary ======================
    // Only needed for multi-class mode to re-index sparse target IDs into dense indices
    if (!useTargetMap) {
      targetMap.clear()
    }

    var newCount = 0 // 本次样本中新增target个数(冷启动物品)
    var existingCount = 0 // 历史累计样本中已存在target个数(老物品)
    var ignoredCount = 0 // 本次样本中新增但被忽略target个数
    var totalOccurrences = 0L // 本次样本中新增样本条数
    var nextIndex = targetMap.size
    for ((targetId, occurrence) <- targetOccurs) {
      totalOccurrences += occurrence
      if (targetMap.contains(targetId)) {
        existingCount += 1
      } else if (occurrence >= targetThreshold) {
        targetMap.put(targetId, nextIndex)
        nextIndex += 1
        newCount += 1
      } else {
        ignoredCount += 1
      }
    }
    green_println(s"new targetMap.size: ${targetMap.size}.")
    green_println(s"target_map: new=${newCount}, existing=${existingCount}, belowThreshold=${ignoredCount}, totalOccurrences=${totalOccurrences}")

    //====================== part2: feature position vocabulary ======================
    green_println(s"old posMap.size: ${posMap.size}.")
    green_println(s"old posDim.size: ${posDim.size}.")

    /** Collect hash statistics across all samples: aggregates sum/powerSum/count per (f_name, f_index, f_type, hash) key */
    val trainSample: Array[((String, Int, Byte, Long), RunningValueStats)] = trainingSample
      .filter(r => r._2)
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        val encoder = featurizer
        val posInfo = new mutable.HashMap[(String, Int, Byte, Long), RunningValueStats]()
        for (sample <- samples) {
          val oneSampleHashArr = getHashInfo(sample, encoder)
          for ((field_name, field_index, field_type, raw, hash, value) <- oneSampleHashArr) {
            val key = (field_name, field_index, field_type, hash)
            posInfo.getOrElseUpdate(key, new RunningValueStats()).add(value)
          }
        }
        posInfo.iterator
      })
      .reduceByKey((a, b) => a.merge(b))
      .collect()
    green_println(s"trainSample.size = ${trainSample.length}")

    // low-frequency values reuse historical positions or are dropped
    var ignoredPosCount = 0 // 本次样本中忽略编码的样本条数
    var reusedHistoryPosCount = 0 // 本次样本中复用编码的样本条数

    /** Shared counter per (f_index, f_type) — ensures features sharing field_index get unique positions */
    val fieldDim = new mutable.HashMap[(Int, Int), Int]()
    posDim.foreach { case ((f_name, f_index, f_type), dim) =>
      val key = (f_index, f_type)
      fieldDim.put(key, math.max(fieldDim.getOrElse(key, 0), dim))
    }
    for (posInfo <- trainSample) {
      val (f_name, f_index, f_type, hash) = posInfo._1
      val dimKey = (f_index, f_type.toInt)
      val stat = posInfo._2
      if (stat.count < featureThreshold) {
        posMap.get((f_index, hash)) match {
          case Some(history) =>
            val currentDim = fieldDim.getOrElse(dimKey, 1)
            fieldDim.put(dimKey, math.max(currentDim, history.pos + 1))
            reusedHistoryPosCount += 1
          case None =>
            ignoredPosCount += 1
        }
      } else {
        if (!fieldDim.contains(dimKey)) {
          posMap.put((f_index, 0L), PosInfo(0))
          fieldDim.put(dimKey, 1)
        }
        val pos: Int = if (f_type == FieldType.Continuous) {
          hash.toInt
        } else if (posMap.contains((f_index, hash))) {
          posMap((f_index, hash)).pos
        } else {
          fieldDim(dimKey)
        }
        val merged: PosInfo = posMap.get((f_index, hash)) match {
          case Some(history) => history.copy(pos = pos).merge(stat)
          case None => PosInfo(pos, stat.sum, stat.powerSum, stat.count)
        }
        posMap.put((f_index, hash), merged)
        val currentDim = fieldDim.getOrElse(dimKey, 1)
        fieldDim.put(dimKey, math.max(currentDim, pos + 1))
      }
    }
    alignFieldDim(posDim, fieldDim, featurizer)
    green_println(s"new posMap.size: ${posMap.size}.")
    green_println(s"new posDim.size: ${posDim.size}.")
    green_println(s"ignored_pos_count: ${ignoredPosCount}. total filtered feature value count")
    green_println(s"reused_history_pos_count: ${reusedHistoryPosCount}. low-frequency feature values reusing historical vocabulary")
  }


  /**
   *
   * @param spark
   * @param yesterday
   * @param trainingSample
   * @param valSample
   * @param testSample
   * @param posMap
   * @param targetMap
   * @param outputDir
   * @param output_format
   */
  def generateSample(spark: SparkSession,
                     yesterday: String,
                     trainingSample: RDD[(T, Boolean)],
                     valSample: RDD[(T, Boolean)],
                     testSample: RDD[(T, Boolean)],
                     posMap: mutable.HashMap[(Int, Long), PosInfo],
                     targetMap: mutable.HashMap[Int, Int],
                     outputDir: String,
                     output_format: String): Unit = {
    val localPosMap = posMap.map { case (k, v) => (k, v.pos) }
    val localTargetMap = if (useTargetMap) targetMap else null
    val basePath = s"${outputDir.stripSuffix("/")}/${yesterday}"
    val splitsToWrite = if (valSample != null) {
      Seq(("train", trainingSample), ("val", valSample), ("test", testSample))
    } else {
      Seq(("", trainingSample))
    }
    for ((suffix, data) <- splitsToWrite) {
      val filterdData = data.filter(r => r._2) // 过滤有效样本
      val subdir = if (suffix.isEmpty) "" else s"/${suffix}"
      if (output_format == "tfrecord" || output_format == "both") {
        sampleWriter.writeTfrecord(filterdData, localPosMap, targetMap, s"${basePath}${subdir}/tfrecord")
      }

      if (output_format == "parquet" || output_format == "both") {
        /** Builds the Parquet schema with (target, *_raw, *_index, *_value) columns for each feature. */
        val parquet_schema: StructType = {
          val fields = new ArrayBuffer[StructField]()
          fields.append(StructField("target", FloatType, nullable = true))
          for ((f_name, _) <- featurizer.getFieldInfo()) {
            fields.append(StructField(f_name + "_raw", ArrayType(StringType, containsNull = false), nullable = true))
            fields.append(StructField(f_name + "_index", ArrayType(LongType, containsNull = false), nullable = true))
            fields.append(StructField(f_name + "_value", ArrayType(FloatType, containsNull = false), nullable = true))
          }
          StructType(fields)
        }
        sampleWriter.writeParquet(spark, filterdData, parquet_schema, localPosMap, localTargetMap, s"${basePath}${subdir}/parquet")
      }
    }
  }

  /**
   * Runs the full pipeline: load samples, build target-map, build pos-map (vocabulary), and write encoded output.
   *
   * @param trainRatio Fraction of data for training (default 1.0 = no split)
   * @param valRatio   Fraction of data for validation (default 0.0). Test ratio = 1.0 - train_ratio - val_ratio
   */
  def run(spark: SparkSession, yesterday: String, featureThreshold: Int, targetThreshold: Int, sampleRatio: Double, posMap: mutable.HashMap[(Int, Long), PosInfo], targetMap: mutable.HashMap[Int, Int], posDim: mutable.HashMap[(String, Int, Int), Int], inputDir: String, trainRatio: Double = 0.8, valRatio: Double = 0.1, parts: Int, outputDir: String, outputFormat: String = "tfrecord"): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    // 1. negative sampling (if necessary)
    val allSamples: RDD[(T, Boolean)] = loadTrainingSamples(spark, inputDir, parts).filter {
        case (sample, _) => keepSample(sample, sampleRatio)
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 2. Time-based split into train/val/test (each split is persisted inside splitSamples)
    val (trainSample, valSample, testSample) = splitSamples(allSamples, trainRatio, valRatio)
    allSamples.unpersist()

    // 3. load and update vocabulary
    generateVocabulary(trainSample, targetThreshold, featureThreshold, targetMap, posMap, posDim)

    // 4. generate final train sample
    generateSample(spark, yesterday, trainSample, valSample, testSample, posMap, targetMap, outputDir, outputFormat)
    trainSample.unpersist()
    if (valSample != null) valSample.unpersist()
    if (testSample != null) testSample.unpersist()

    // Print consolidated quality report before returning
    qualityTracker.printReport()

    // 5. return new vocabulary
    (posMap, targetMap, posDim)
  }
}
