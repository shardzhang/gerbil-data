package pipeline

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag
import utils.LogUtils.green_println
import featurizer.core.{FieldType, Featurizer}
import pipeline.serde.{PosMapSerDe, SampleWriter}
import pipeline.stats.{DataQualityTracker, PosInfo, RunningValueStats}


/**
 * End-to-end training sample generation pipeline.
 *
 * Orchestrates the full workflow from raw samples to featurized TFRecord/Parquet output:
 *
 *  1. Load and parse raw samples from the input directory
 *  2. Time-based train/val/test split to prevent data leakage
 *  3. Compute target distribution and build target-map (frequency thresholding)
 *  4. Collect hash statistics across all samples, build embedding vocabulary (pos-map)
 *  5. Encode each sample via the Featurizer and write to TFRecord or Parquet
 *  6. Report data quality metrics (parse success rate, target distribution, etc.)
 *
 * Supports incremental training: pos-map and target-map from previous runs
 * can be loaded and updated with new data.
 *
 * @tparam T the training sample type, must have a ClassTag for Spark serialization
 */
abstract class Pipeline[T: ClassTag] extends Serializable {
  @transient var hadoopConf: Configuration = new Configuration()
  /** Persists/restores position-map and target-map across runs. */
  @transient val posMapSerDe: PosMapSerDe = new PosMapSerDe(hadoopConf)
  /** Serializes featurized samples to TFRecord or Parquet. */
  @transient lazy val writer: SampleWriter[T] = new SampleWriter[T](() => feature_encoder, max_dim)
  /** Tracks record counts, parse success rates, and target distributions at each ETL stage. */
  @transient lazy val qualityTracker: DataQualityTracker = new DataQualityTracker()

  /** Maximum hash dimension (typically 2^60 for 64-bit embedding space). */
  def max_dim: Long

  /** The featurizer that converts raw samples into encoded features. */
  def feature_encoder: Featurizer[T]

  /** Loads and parses raw training samples from the given input directory. */
  def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(T, Boolean)]

  /** Extracts the target label (as Int) from a sample for threshold statistics. */
  def getSampleTarget(sample: T): Int

  /** Extracts the timestamp (millis) from a sample for time-based split. */
  def getSampleTimestamp(sample: T): Long

  /**
   * Whether to re-encode target values through target_map (sequential indexing).
   * Override to return false when raw target values should be written directly.
   */
  def useTargetMap: Boolean = true

  /** Down-samples negative samples (target == 0) by `sample_ratio`. Positive samples are always kept. */
  def keepSample(sample: T, sample_ratio: Double): Boolean

  /** Computes hash info (field name, index, type, raw value, hash, value) for vocabulary building. */
  def getHashInfo(sample: T, encoder: Featurizer[T]): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.getHashInfo(sample, max_dim)
  }

  /** Aligns posDim entries so all features sharing the same (f_index, f_type) use the same dim. */
  def alignPosDim(posDim: HashMap[(String, Int, Int), Int],
                           posCounter: HashMap[(Int, Int), Int],
                           featurizer: Featurizer[_]): Unit = {
    val nameIndexMap = featurizer.getFieldInfo()
      .groupBy(_._2)
      .map { case (idx, entries) => (idx, entries.map(_._1)) }

    posDim.clear()
    posCounter.foreach { case ((f_index, f_type), dim) =>
      val f_names = nameIndexMap.getOrElse(f_index, Nil)
      f_names.foreach { f_name =>
        posDim.put((f_name, f_index, f_type), dim)
      }
    }
  }

  /** Builds the Parquet schema with (target, *_raw, *_index, *_value) columns for each feature. */
  def parquet_schema: StructType = {
    val fields = new ArrayBuffer[StructField]()
    fields.append(StructField("target", FloatType, nullable = true))
    for ((f_name, _) <- feature_encoder.getFieldInfo()) {
      fields.append(StructField(f_name + "_raw", ArrayType(StringType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_index", ArrayType(LongType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_value", ArrayType(FloatType, containsNull = false), nullable = true))
    }
    StructType(fields)
  }

  /** Runs the full pipeline: load samples, build target-map, build pos-map (vocabulary), and write encoded output.
    *
    * @param trainRatio Fraction of data for training (default 1.0 = no split)
    * @param valRatio   Fraction of data for validation (default 0.0). Test ratio = 1.0 - train_ratio - val_ratio
    */
  def run(spark: SparkSession,
          yesterday: String,
          /** Minimum occurrence count for a feature value to be included in vocabulary. */
          featureThreshold: Int,
          /** Minimum occurrence count for a target value to be included in target-map. */
          targetThreshold: Int,
          /** Down-sampling ratio for negative samples (0.0 = keep none, 1.0 = keep all). */
          sampleRatio: Double,
          /** HashMap[(f_index, hash), PosInfo] */
          posMap: HashMap[(Int, Long), PosInfo],
          /** HashMap[target, pos] */
          targetMap: HashMap[Int, Int],
          /** HashMap[(f_name, f_index, f_type), dim] */
          posDim: HashMap[(String, Int, Int), Int],
          inputDir: String,
          outputDir: String,
          parts: Int,
          output_format: String = "tfrecord",
          trainRatio: Double = 1.0,
          valRatio: Double = 0.0
         ): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    val allSamples: RDD[(T, Boolean)] = loadTrainingSamples(spark, inputDir, parts)
      .filter {
        case (sample, _) => keepSample(sample, sampleRatio)
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Time-based split into train/val/test
    val (trainingSample, valSample, testSample) = if (trainRatio >= 1.0) {
      (allSamples, null, null)
    } else {
      val sorted = allSamples.sortBy { case (sample, _) => getSampleTimestamp(sample) }
      val indexed = sorted.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
      val total = indexed.count()
      val trainEnd = (total * trainRatio).toLong
      val valEnd = trainEnd + (total * valRatio).toLong

      val train = indexed.filter { case (_, idx) => idx < trainEnd }.map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val valid = indexed.filter { case (_, idx) => idx >= trainEnd && idx < valEnd }.map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val test = indexed.filter { case (_, idx) => idx >= valEnd }.map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val trainCount = train.count()
      val valCount = valid.count()
      val testCount = test.count()
      indexed.unpersist()
      allSamples.unpersist()
      green_println(s"train/val/test split: ${total} total, " +
        s"train=${trainCount} (${trainRatio * 100}%), " +
        s"val=${valCount} (${valRatio * 100}%), " +
        s"test=${testCount} (${(1.0 - trainRatio - valRatio) * 100}%)")
      // Record split counts to quality tracker
      qualityTracker.recordCounts("train_split", trainCount)
      qualityTracker.recordCounts("val_split", valCount)
      qualityTracker.recordCounts("test_split", testCount)
      (train, valid, test)
    }

    // Count parse success vs. failure
    val ret_counts = trainingSample
      .map(s => (s._2, 1))
      .reduceByKey(_ + _)
      .collect()

    val totalCount = ret_counts.map(_._2).sum
    val validCount = ret_counts.find(_._1).map(_._2.toLong).getOrElse(0L)  // count of successfully parsed samples
    for (ret <- ret_counts) {
      green_println(s"ret_counts ${ret._1} ${ret._2}")
    }

    // Aggregate sample counts per target value
    val sample_num: Array[(Int, Int)] = trainingSample
      .map { case (sample, _) => sample }
      .map(sample => (getSampleTarget(sample), 1))
      .reduceByKey(_ + _)
      .collect()
      .sortWith((a, b) => a._2 > b._2)

    // Record train quality: total, parse success, target distribution
    qualityTracker.record("train_stats", totalCount, validCount, sample_num.toSeq)
    /** binary
     * Stage: train_stats
     * Total:      800167
     * Valid:      800167 (100.00%)
     * Targets:    2 distinct
     * Top-5:      1=463020, 0=337147
     */
    /** rating
     * Stage: train_stats
     * Total:      800167
     * Valid:      800167 (100.00%)
     * Targets:    5 distinct
     * Top-5:      4=278240, 3=207190, 5=184780, 2=84619, 1=45338
     */
    // Build target-map: assign a sequential index to each target value meeting the threshold
    // Only needed for multi-class mode to re-index sparse target IDs into dense indices
    var newCount = 0
    var existingCount = 0
    var ignoredCount = 0
    var totalOccurrences = 0L

    if (!useTargetMap) {
      targetMap.clear()
    } else {
      var nextIndex = targetMap.size
      for ((targetId, occurrence) <- sample_num) {
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
      green_println(s"target_map: new=${newCount}, existing=${existingCount}, belowThreshold=${ignoredCount}, total=${totalOccurrences}")
    }

    // Collect hash statistics across all samples: aggregates sum/powerSum/count per (field, hash) key
    /** Array[(f_name, f_index, f_type, hash)] */
    val trainSample: Array[((String, Int, Byte, Long), RunningValueStats)] = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        val encoder = feature_encoder
        val posHash: mutable.Map[(String, Int, Byte, Long), RunningValueStats] = new mutable.HashMap[(String, Int, Byte, Long), RunningValueStats]()
        for (sample <- samples) {
          val oneSampleHashArr = getHashInfo(sample, encoder)
          for ((field_name, field_index, field_type, _, hash, value) <- oneSampleHashArr) {
            val key = (field_name, field_index, field_type, hash)
            posHash.getOrElseUpdate(key, new RunningValueStats()).add(value)
          }
        }
        posHash.iterator
      })
      .reduceByKey((a, b) => a.merge(b))
      .collect()
    green_println(s"pos_arr.size = ${trainSample.length}")

    // Build position-map: assign embedding indices to frequent feature values;
    // low-frequency values reuse historical positions or are dropped
    var ignoredPosCount = 0
    var reusedHistoryPosCount = 0

    /** HashMap[(f_index, hash), pos] */
    val posMapLocal = new HashMap[(Int, Long), Int]

    /** Shared counter per (f_index, f_type) — ensures features sharing field_index get unique positions */
    val posCounter = new HashMap[(Int, Int), Int]()
    posDim.foreach { case ((_, f_index, f_type), dim) =>
      val key = (f_index, f_type)
      posCounter.put(key, math.max(posCounter.getOrElse(key, 0), dim))
    }
    for (posInfo <- trainSample) {
      val (f_name, f_index, f_type, hash) = posInfo._1
      val dimKey = (f_index, f_type.toInt)
      val stat = posInfo._2
      if (stat.count < featureThreshold) {
        posMap.get((f_index, hash)) match {
          case Some(history) =>
            posMapLocal.put((f_index, hash), history.pos)
            val currentDim = posCounter.getOrElse(dimKey, 1)
            posCounter.put(dimKey, math.max(currentDim, history.pos + 1))
            reusedHistoryPosCount += 1
          case None =>
            ignoredPosCount += 1
        }
      } else {
        if (!posCounter.contains(dimKey)) {
          posCounter.put(dimKey, 1)
          posMap.put((f_index, 0L), PosInfo(0))
        }
        val pos = if (f_type == FieldType.Continuous) {
          if (hash <= 0L || hash > Int.MaxValue.toLong) {
            throw new IllegalArgumentException(s"Continuous feature ${f_name}[${f_index}] position must be in [1, ${Int.MaxValue}], got ${hash}")
          }
          hash.toInt
        } else if (posMap.contains((f_index, hash))) {
          posMap((f_index, hash)).pos
        } else {
          posCounter(dimKey)
        }
        val merged: PosInfo = posMap.get((f_index, hash)) match {
          case Some(history) => history.copy(pos = pos).merge(stat)
          case None => PosInfo(pos, stat.sum, stat.powerSum, stat.count)
        }
        posMap.put((f_index, hash), merged)
        posMapLocal.put((f_index, hash), pos)
        val currentDim = posCounter.getOrElse(dimKey, 1)
        posCounter.put(dimKey, math.max(currentDim, pos + 1))
      }
    }
    alignPosDim(posDim, posCounter, feature_encoder)

    green_println(s"ignored_pos_count: ${ignoredPosCount}. total filtered feature value count")
    green_println(s"reused_history_pos_count: ${reusedHistoryPosCount}. low-frequency feature values reusing historical vocabulary")
    green_println(s"posMapLocal: ${posMapLocal.size}. valid feature value count")
    green_println(s"pos_map: ${posMap.size}. accumulated feature value count")
    green_println(s"target_map: ${targetMap.size}. accumulated target count")
    green_println(s"pos_dim: ${posDim.size}. accumulated feature domain count")

    val basePath = s"${outputDir.stripSuffix("/")}/${yesterday}"
    val posMapLocalImmutable: collection.Map[(Int, Long), Int] = posMapLocal
    val targetMapImmutable: collection.Map[Int, Int] = if (useTargetMap) targetMap else null

    val splitsToWrite = if (valSample != null) {
      Seq(("train", trainingSample), ("val", valSample), ("test", testSample))
    } else {
      Seq(("", trainingSample))
    }
    for ((suffix, data) <- splitsToWrite) {
      val subdir = if (suffix.isEmpty) "" else s"/${suffix}"
      if (output_format == "tfrecord" || output_format == "both") {
        writer.writeTfrecord(data, posMapLocalImmutable, targetMapImmutable, s"${basePath}${subdir}/tfrecord")
      }
      if (output_format == "parquet" || output_format == "both") {
        writer.writeParquet(spark, data, parquet_schema, posMapLocalImmutable, targetMapImmutable, s"${basePath}${subdir}/parquet")
      }
    }
    // Print consolidated quality report before returning
    qualityTracker.printReport()
    (posMap, targetMap, posDim)
  }
}
