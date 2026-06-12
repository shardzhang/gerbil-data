package pipeline

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag
import utils.LogUtils.green_println
import featurizer.core.{FeatureType, Featurizer}
import pipeline.serde.{PosMapSerDe, SampleWriter}
import pipeline.stats.{DataQualityTracker, PosInfo, RunningValueStats}

/** Orchestrates the end-to-end training sample generation pipeline: load samples, build vocabulary (pos-map/target-map), and encode features into TFRecord/Parquet. */
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
  // lazy val -> 单例模式(所有线程共享), 导致竞态损坏
  // def -> 每次调用创建新实例
  def feature_encoder: Featurizer[T]

  /** Loads and parses raw training samples from the given input directory. */
  def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(T, Boolean)]

  /** Extracts the target label (as Int) from a sample for threshold statistics. */
  def getSampleTarget(sample: T): Int

  /** Extracts the timestamp (millis) from a sample for time-based split. */
  def getSampleTimestamp(sample: T): Long

  /** Down-samples negative samples (target == 0) by `sample_ratio`. Positive samples are always kept. */
  def keepSample(sample: T, sample_ratio: Double): Boolean = {
    getSampleTarget(sample) != 0 || ThreadLocalRandom.current().nextDouble() <= sample_ratio
  }

  /** Computes hash info (field name, index, type, raw value, hash, value) for vocabulary building. */
  def getHashInfo(sample: T, encoder: Featurizer[T]): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.get_hash_info(sample, max_dim)
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
    * @param train_ratio Fraction of data for training (default 1.0 = no split)
    * @param val_ratio   Fraction of data for validation (default 0.0). Test ratio = 1.0 - train_ratio - val_ratio
    */
  def run(spark: SparkSession,
          yesterday: String,
          /** Minimum occurrence count for a feature value to be included in vocabulary. */
          feature_threshold: Int,
          /** Minimum occurrence count for a target value to be included in target-map. */
          target_threshold: Int,
          /** Down-sampling ratio for negative samples (0.0 = keep none, 1.0 = keep all). */
          sample_ratio: Double,
          pos_map: HashMap[(Int, Long), PosInfo],
          target_map: HashMap[Int, Int],
          pos_dim: HashMap[(String, Int, Int), Int],
          input_dir: String,
          output_dir: String,
          parts: Int,
          output_format: String = "tfrecord",
          train_ratio: Double = 1.0,
          val_ratio: Double = 0.0
         ): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    val allSamples: RDD[(T, Boolean)] = loadTrainingSamples(spark, input_dir, parts)
      .filter {
        case (sample, _) => keepSample(sample, sample_ratio)
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Time-based split into train/val/test
    val (trainingSample, valSample, testSample) = if (train_ratio >= 1.0) {
      (allSamples, null, null)
    } else {
      val sorted = allSamples.sortBy { case (sample, _) => getSampleTimestamp(sample) }
      val indexed = sorted.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
      val total = indexed.count()
      val trainEnd = (total * train_ratio).toLong
      val valEnd = trainEnd + (total * val_ratio).toLong

      val train = indexed.filter { case (_, idx) => idx < trainEnd }.map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val valid = indexed.filter { case (_, idx) => idx >= trainEnd && idx < valEnd }.map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val test = indexed.filter { case (_, idx) => idx >= valEnd }.map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      indexed.unpersist()
      allSamples.unpersist()

      val trainCount = train.count()
      val valCount = valid.count()
      val testCount = test.count()
      green_println(s"train/val/test split: ${total} total, " +
        s"train=${trainCount} (${train_ratio*100}%), " +
        s"val=${valCount} (${val_ratio*100}%), " +
        s"test=${testCount} (${(1.0-train_ratio-val_ratio)*100}%)")
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

    // Build target-map: assign a sequential index to each target value meeting the threshold
    var index = if (target_map.isEmpty) {
      0
    } else {
      target_map.size
    }

    var total_number = 0
    var newTargetCount = 0
    var existingTargetCount = 0
    var ignoredTargetCount = 0
    for ((target_id, occurrence) <- sample_num) {
      if (occurrence >= target_threshold && !target_map.contains(target_id)) {
        target_map.put(target_id, index)
        index = index + 1
        newTargetCount += 1
      } else if (target_map.contains(target_id)) {
        existingTargetCount += 1
      } else {
        ignoredTargetCount += 1
      }
      total_number += occurrence
    }
    green_println(s"new targets added: ${newTargetCount}")
    green_println(s"existing targets reused: ${existingTargetCount}")
    green_println(s"targets below threshold: ${ignoredTargetCount}")
    green_println(s"total target number: ${total_number}")

    // Collect hash statistics across all samples: aggregates sum/powerSum/count per (field, hash) key
    val train_sample_hash_arr = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        // 改为 def 或工厂函数后, 每个 partition 创建独立的 featurizer 实例, 消除共享, 竞态消失
        val encoder = feature_encoder
        val pos_hash = new mutable.HashMap[(String, Int, Byte, Long), RunningValueStats]()
        for (sample <- samples) {
          val one_sample_hash_array = getHashInfo(sample, encoder)
          for ((f_name, f_index, f_type, _, hash, value) <- one_sample_hash_array) {
            val key = (f_name, f_index, f_type, hash)
            pos_hash.getOrElseUpdate(key, new RunningValueStats()).add(value)
          }
        }
        pos_hash.iterator
      })
      .reduceByKey((a, b) => a.merge(b))
      .collect()
    green_println(s"pos_arr.size = ${train_sample_hash_arr.length}")

    // Build position-map: assign embedding indices to frequent feature values;
    // low-frequency values reuse historical positions or are dropped
    var ignoredPosCount = 0
    var reusedHistoryPosCount = 0
    val pos_map_local = new HashMap[(Int, Long), Int]
    for (pos_info <- train_sample_hash_arr) {
      val (f_name, f_index, f_type, hash) = pos_info._1
      val fieldKey = (f_name, f_index, f_type.toInt)
      val stat = pos_info._2
      if (stat.count < feature_threshold) {
        pos_map.get((f_index, hash)) match {
          case Some(history) =>
            pos_map_local.put((f_index, hash), history.pos)
            val currentDim = pos_dim.getOrElse(fieldKey, 1)
            pos_dim.put(fieldKey, math.max(currentDim, history.pos + 1))
            reusedHistoryPosCount += 1
          case None =>
            ignoredPosCount += 1
        }
      } else {
        if (!pos_dim.contains(fieldKey)) {
          pos_dim.put(fieldKey, 1)
          pos_map.put((f_index, 0L), PosInfo(0))
        }

        val pos = if (f_type == FeatureType.Continuous) {
          if (hash <= 0L || hash > Int.MaxValue.toLong) {
            throw new IllegalArgumentException(s"Continuous feature ${f_name}[${f_index}] position must be in [1, ${Int.MaxValue}], got ${hash}")
          }
          hash.toInt
        } else if (pos_map.contains((f_index, hash))) {
          pos_map((f_index, hash)).pos
        } else {
          pos_dim(fieldKey)
        }
        val merged: PosInfo = pos_map.get((f_index, hash)) match {
          case Some(history) => history.copy(pos = pos).merge(stat)
          case None => PosInfo(pos, stat.sum, stat.powerSum, stat.count)
        }
        pos_map.put((f_index, hash), merged)
        pos_map_local.put((f_index, hash), pos)
        val currentDim = pos_dim.getOrElse(fieldKey, 1)
        pos_dim.put(fieldKey, math.max(currentDim, pos + 1))
      }
    }
    green_println(s"ignored_pos_count: ${ignoredPosCount}. total filtered feature value count")
    green_println(s"reused_history_pos_count: ${reusedHistoryPosCount}. low-frequency feature values reusing historical vocabulary")
    green_println(s"pos_map_local: ${pos_map_local.size}. valid feature value count")
    green_println(s"pos_map: ${pos_map.size}. accumulated feature value count")
    green_println(s"target_map: ${target_map.size}. accumulated target count")
    green_println(s"pos_dim: ${pos_dim.size}. accumulated feature domain count")

    val basePath = s"${output_dir.stripSuffix("/")}/${yesterday}"
    val parquetSchema = parquet_schema
    val parquetFieldNames = parquetSchema.fieldNames.toSeq
    val posMapLocalImmutable: collection.Map[(Int, Long), Int] = pos_map_local
    val targetMapImmutable: collection.Map[Int, Int] = target_map

    val splitsToWrite = if (valSample != null) {
      Seq(("train", trainingSample), ("val", valSample), ("test", testSample))
    } else {
      Seq(("", trainingSample))
    }

    for ((suffix, data) <- splitsToWrite) {
      val subdir = if (suffix.isEmpty) "" else s"/${suffix}"
      if (output_format == "parquet" || output_format == "both") {
        writer.writeParquet(data, spark, parquetSchema, parquetFieldNames, posMapLocalImmutable, targetMapImmutable, s"${basePath}${subdir}/parquet")
      }
      if (output_format == "tfrecord" || output_format == "both") {
        writer.writeTfrecord(data, posMapLocalImmutable, targetMapImmutable, s"${basePath}${subdir}/tfrecord")
      }
    }

    // Print consolidated quality report before returning
    qualityTracker.printReport()

    (pos_map, target_map, pos_dim)
  }
}
