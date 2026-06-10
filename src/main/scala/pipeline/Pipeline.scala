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
import pipeline.serde.{PosMapSerDe, SampleSerDe}
import pipeline.stats.{PosInfo, RunningValueStats}

abstract class Pipeline[T: ClassTag] extends Serializable {
  def max_dim: Long

  def feature_encoder: Featurizer[T]

  @transient var hadoopConf: Configuration = new Configuration()

  @transient val posMapSerDe: PosMapSerDe = new PosMapSerDe(hadoopConf)
  @transient lazy val sampleSerDe: SampleSerDe[T] = new SampleSerDe[T](feature_encoder, max_dim)

  def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(T, Boolean)]

  def getSampleTarget(sample: T): Int

  def keepSample(sample: T, sample_ratio: Double): Boolean = {
    getSampleTarget(sample) != 0 || ThreadLocalRandom.current().nextDouble() <= sample_ratio
  }

  def getHashInfo(sample: T, encoder: Featurizer[T]): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.get_hash_info(sample, max_dim)
  }

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

  def run(spark: SparkSession,
          yesterday: String,
          feature_threshold: Int,
          target_threshold: Int,
          sample_ratio: Double,
          pos_map: HashMap[(Int, Long), PosInfo],
          target_map: HashMap[Int, Int],
          pos_dim: HashMap[(String, Int, Int), Int],
          input_dir: String,
          output_dir: String,
          parts: Int,
          output_format: String = "tfrecord"
         ): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    val trainingSample: RDD[(T, Boolean)] = loadTrainingSamples(spark, input_dir, parts)
      .filter {
        case (sample, _) => keepSample(sample, sample_ratio)
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val ret_counts = trainingSample
      .map(s => (s._2, 1))
      .reduceByKey(_ + _)
      .collect()

    for (ret <- ret_counts) {
      green_println(s"ret_counts ${ret._1} ${ret._2}")
    }

    val sample_num: Array[(Int, Int)] = trainingSample
      .map { case (sample, _) => sample }
      .map(sample => (getSampleTarget(sample), 1))
      .reduceByKey(_ + _)
      .collect()
      .sortWith((a, b) => a._2 > b._2)

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

    val train_sample_hash_arr = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
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

    val tfRecordPath = s"${output_dir.stripSuffix("/")}/${yesterday}/tfrecord"
    val parquetPath = s"${output_dir.stripSuffix("/")}/${yesterday}/parquet"
    val parquetSchema = parquet_schema
    val parquetFieldNames = parquetSchema.fieldNames.toSeq
    val posMapLocalImmutable: collection.Map[(Int, Long), Int] = pos_map_local
    val targetMapImmutable: collection.Map[Int, Int] = target_map

    if (output_format == "parquet" || output_format == "both") {
      sampleSerDe.writeParquet(trainingSample, spark, parquetSchema, parquetFieldNames, posMapLocalImmutable, targetMapImmutable, parquetPath)
    }

    if (output_format == "tfrecord" || output_format == "both") {
      sampleSerDe.writeTfrecord(trainingSample, posMapLocalImmutable, targetMapImmutable, tfRecordPath)
    }

    (pos_map, target_map, pos_dim)
  }
}
