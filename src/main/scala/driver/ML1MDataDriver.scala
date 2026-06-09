package driver

import org.json.{JSONArray, JSONObject}
import org.apache.spark.sql.types._
import org.tensorflow.example.Example
import sample.ML1MTrainSample
import encoder.FeatureEncoder4ML1M
import encoder.vectorizer.{FeatureEncoder, FeatureType}
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import utils.LogUtils.green_println
import utils.LogUtils.setLogLevel
import utils.ParquetRecord
import com.google.common.io.{LittleEndianDataInputStream, LittleEndianDataOutputStream}

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, DataInputStream, DataOutputStream, InputStreamReader, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.ThreadLocalRandom
import scala.collection.compat.MapFactoryExtensionMethods
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * @author shard zhang
 * @date 2026/6/5 11:38
 * @note
 *
 * 基于TFRecord的样本生成, 包括:
 *
 *      1. TFRecord文件
 *         2. nn_pos_map.json编码表(用于离线模型训练)
 *         3. nn_pos_map.bin编码表(用于在线模型推理)
 */
final class FeatureStat(var sum: Double, var powerSum: Double, var count: Long) extends Serializable {
  def add(value: Float): FeatureStat = {
    val valueDouble = value.toDouble
    sum += valueDouble
    powerSum += valueDouble * valueDouble
    count += 1L
    this
  }
  def merge(other: FeatureStat): FeatureStat = {
    sum += other.sum
    powerSum += other.powerSum
    count += other.count
    this
  }
}

final case class PosInfo(pos: Int, sum: Double, powerSum: Double, count: Long) extends Serializable {
  // mean. E(x) = sum / count
  // variance. D(x) = E(x^2) - E^2(x) = power_sum / count - power(sum / count, 2)
  def mean: Double = {
    if (count <= 0L) {
      return 0.0D
    }
    sum / count.toDouble
  }
  def std: Double = {
    if (count <= 0L) {
      return 1.0D
    }
    val variance = math.max(powerSum * 1.0 / count - math.pow(mean, 2), 0.0D)
    math.sqrt(variance + 0.000001D)
  }
  def merge(stats: FeatureStat): PosInfo = {
    PosInfo(pos, sum + stats.sum, powerSum + stats.powerSum, count + stats.count)
  }
}

object ML1MDataDriver extends Serializable {
  val max_dim: Long = 1L << 60

  private def readText(reader: BufferedReader): String = {
    val content = new StringBuilder()
    var line = reader.readLine()
    var firstLine = true
    while (line != null) {
      if (!firstLine) {
        content.append('\n')
      }
      content.append(line)
      firstLine = false
      line = reader.readLine()
    }
    content.toString()
  }

  private def legacyPosInfo(pos: Int, mean: Double, std: Double, count: Long): PosInfo = {
    val safeCount = math.max(count, 1L)
    val variance = math.max(std * std - 0.000001D, 0.0D)
    val sum = mean * safeCount.toDouble
    val powerSum = (variance + mean * mean) * safeCount.toDouble
    PosInfo(pos, sum, powerSum, safeCount)
  }

  private def ensureFieldDim(posDimMap: mutable.HashMap[(String, Int, Int), Int],
                             fieldKey: (String, Int, Int),
                             pos: Int): Unit = {
    val currentDim = posDimMap.getOrElse(fieldKey, 1)
    posDimMap.put(fieldKey, math.max(currentDim, pos + 1))
  }

  private def exportMeanStd(f_type: Int, stat: PosInfo): (Double, Double) = {
    if (f_type == FeatureType.Categorical) {
      return (0.0D, 1.0D)
    }
    (stat.mean, stat.std)
  }

  /**
   *
   * @param path
   * @param posMap Map[(f_index, entry.getLong("hash")), posInfo)
   * @param targetMap Map[target_id, pos]
   * @param posDimMap Map[(f_name, f_index, f_type), dim]
   * @return
   */
  private def restoreFromJson(path: String,
                                      yesterday: String,
                                      posMap: mutable.HashMap[(Int, Long), PosInfo],
                                      targetMap: mutable.HashMap[Int, Int],
                                      posDimMap: mutable.HashMap[(String, Int, Int), Int]): Boolean = {
    val jsonPath = s"${path.stripSuffix("/")}/nn_pos_map.json"
    val fs = FileSystem.get(URI.create(jsonPath), new Configuration())
    val file = new Path(jsonPath)
    if (!fs.exists(file)) {
      return false
    }

    val reader = new BufferedReader(new InputStreamReader(fs.open(file), "utf-8"))
    try {
      val root = new JSONObject(readText(reader))
      val targets = root.optJSONObject("targets")
      if (targets != null) {
        val keys = targets.keys()
        while (keys.hasNext) {
          val rawTarget = keys.next()
          targetMap.put(rawTarget.toInt, targets.getInt(rawTarget))
        }
      }

      val features = root.optJSONArray("features")
      var featureIndex = 0
      while (features != null && featureIndex < features.length()) {
        val feature = features.getJSONObject(featureIndex)
        val f_name = feature.getString("field_name")
        val f_index = feature.getInt("field_index")
        val f_type = feature.getInt("field_type")
        posDimMap.put((f_name, f_index, f_type), feature.getInt("dim"))

        val entries = feature.optJSONArray("entries")
        var entryIndex = 0
        while (entries != null && entryIndex < entries.length()) {
          val entry = entries.getJSONObject(entryIndex)
          val posInfo = if (entry.has("sum") && entry.has("power_sum") && entry.has("count")) {
            PosInfo(
              pos = entry.getInt("pos"),
              sum = entry.getDouble("sum"),
              powerSum = entry.getDouble("power_sum"),
              count = entry.getLong("count")
            )
          } else {
            val count = if (entry.has("count")) entry.getLong("count") else 1L
            val mean = if (entry.has("mean")) entry.getDouble("mean") else 0.0D
            val std = if (entry.has("std")) entry.getDouble("std") else 1.0D
            legacyPosInfo(entry.getInt("pos"), mean, std, count)
          }
          posMap.put((f_index, entry.getLong("hash")), posInfo)
          entryIndex = entryIndex + 1
        }
        featureIndex = featureIndex + 1
      }
    } finally {
      reader.close()
    }
    true
  }

  private def restoreFromText(path: String,
                              posDimMap: mutable.HashMap[(String, Int, Int), Int]): Boolean = {
    val textPath = s"${path}/nn_pos_map.txt"
    val fs = FileSystem.get(URI.create(textPath), new Configuration())
    val file = new Path(textPath)
    if (!fs.exists(file)) {
      return false
    }
    val reader = new BufferedReader(new InputStreamReader(fs.open(file), "utf-8"))
    try {
      var line = reader.readLine()
      line = reader.readLine()
      while (line != null) {
        val items = line.split(",")
        if (items.length >= 4) {
          posDimMap.put((items(0), items(1).toInt, items(2).toInt), items(3).toInt)
        }
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    true
  }

  private def readIntLE(reader: DataInputStream): Int ={
    // Java的DataInputStream 默认大端序
    Integer.reverseBytes(reader.readInt())
  }

  private def readLongLE(reader: DataInputStream): Long = java.lang.Long.reverseBytes(reader.readLong())

  private def readDoubleLE(reader: DataInputStream): Double = java.lang.Double.longBitsToDouble(readLongLE(reader))

  private def writeIntLE(writer: DataOutputStream, value: Int): Unit = writer.writeInt(Integer.reverseBytes(value))

  private def writeLongLE(writer: DataOutputStream, value: Long): Unit = writer.writeLong(java.lang.Long.reverseBytes(value))

  private def writeDoubleLE(writer: DataOutputStream, value: Double): Unit = writeLongLE(writer, java.lang.Double.doubleToLongBits(value))

  def feature_encoder: FeatureEncoder[ML1MTrainSample] = {
    new FeatureEncoder4ML1M().setup()
  }

  /**
   * List[(f_name, f_index, f_type, format, hash, value)]
   */
  def get_hash_info_from_a_sample(sample: ML1MTrainSample,
                                 encoder: FeatureEncoder[ML1MTrainSample]): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.get_hash_info(sample, max_dim)
  }

  /**
   * 解析样本为TFRecord格式
   */
  def parse_a_sample_tfrecord(sample: ML1MTrainSample,
                              encoder: FeatureEncoder[ML1MTrainSample],
                              pos_map: immutable.HashMap[(Int, Long), Int],
                              target_map: immutable.HashMap[Int, Int]): (Example, Boolean, Boolean) = {
    val builder = Example.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }

  /**
   * 解析样本为Parquet格式
   */
  def parse_a_sample_parquet(sample: ML1MTrainSample,
                             encoder: FeatureEncoder[ML1MTrainSample],
                             pos_map: immutable.HashMap[(Int, Long), Int],
                             target_map: immutable.HashMap[Int, Int]): (ParquetRecord, Boolean, Boolean) = {
    val builder = ParquetRecord.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }

  /**
   * Parquet格式的Schema
   */
  def parquet_schema: StructType = {
    val fields = new ArrayBuffer[StructField]()
    fields.append(StructField("target", FloatType, nullable = true))
    for ((f_name, _) <- feature_encoder.get_field_name_and_index()) {
      fields.append(StructField(f_name + "_index", ArrayType(LongType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_value", ArrayType(FloatType, containsNull = false), nullable = true))
    }
    StructType(fields.toSeq)
  }

  def restore_pos_map(path: String, yesterday: String): (
      HashMap[(Int, Long), PosInfo],
      HashMap[Int, Int],
      HashMap[(String, Int, Int), Int]
    ) = {

    /** Map((f_name, f_index, f_type), dim) */
    val pos_dim_map = new mutable.HashMap[(String, Int, Int), Int]()
    /** Map((f_index, hash), posInfo) */
    val pos_map = new mutable.HashMap[(Int, Long), PosInfo]()
    /** Map(target, pos) */
    val target_map = new mutable.HashMap[Int, Int]()

    // nn_pos_map.json
    try {
      if (!restoreFromJson(path, yesterday, pos_map, target_map, pos_dim_map)) {
        restoreFromText(path, pos_dim_map)
      }
    } catch {
      case e: Throwable =>
        green_println(e.toString)
        try {
          pos_map.clear()
          target_map.clear()
          pos_dim_map.clear()
          restoreFromText(path, pos_dim_map)
        } catch {
          case legacyError: Throwable => green_println(legacyError.toString)
        }
    }

    // nn_pos_map.bin
    if (pos_map.isEmpty || target_map.isEmpty) try {
      val binPath = s"${path.stripSuffix("/")}/nn_pos_map.bin"
      val fs = FileSystem.get(URI.create(binPath), new Configuration())
      val reader = new DataInputStream(new BufferedInputStream(fs.open(new Path(binPath))))
      val timestamp = readLongLE(reader)
      var size = readIntLE(reader)
      green_println(s"timestamp: ${timestamp}")
      green_println(s"pos_map size: ${size}")
      // index不保序. 即不是从0开始一致增大,可能是乱序的, 但是pos和index一定是正确对应的
      while (size > 0) {
        val fIndex = readIntLE(reader)
        val hash = readLongLE(reader)
        val pos = readIntLE(reader)
        val mean = readDoubleLE(reader)
        val std = readDoubleLE(reader)
        if (!pos_map.contains((fIndex, hash))) {
          pos_map.put((fIndex, hash), legacyPosInfo(pos, mean, std, 1L))
        }
        size = size - 1
      }
      size = readIntLE(reader)
      green_println(s"target_map size = ${size}")
      while (size > 0) {
        val rawTarget = readIntLE(reader)
        val encodedTarget = readIntLE(reader)
        if (!target_map.contains(rawTarget)) {
          target_map.put(rawTarget, encodedTarget)
        }
        size = size - 1
      }
      reader.close()
    } catch {
      case e: Throwable => println(e.toString)
    }
    green_println(s"read pos_map size = ${pos_map.size}")
    green_println(s"read target_map size = ${target_map.size}")
    green_println(s"read pos_dim_map size = ${pos_dim_map.size}")
    (pos_map, target_map, pos_dim_map)
  }

  def save_pos_map(path: String,
                   pos_map: immutable.HashMap[(Int, Long), PosInfo],
                   target_map: immutable.HashMap[Int, Int],
                   pos_dim: immutable.HashMap[(String, Int, Int), Int]): Unit = {
    // nn_pos_map.json
    do {
      val jsonPath = s"${path.stripSuffix("/")}/nn_pos_map.json"
      val fs = FileSystem.get(URI.create(jsonPath), new Configuration())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(jsonPath), true), "utf-8"))

      try {
        val root = new JSONObject()
        root.put("target_size", target_map.size)
        val targets = new JSONObject()
        target_map.toSeq.sortBy(_._2).foreach { case (rawItemId, encodedId) =>
          targets.put(rawItemId.toString, encodedId)
        }
        root.put("targets", targets)
        val features = new JSONArray()

        /** Map(f_index, (f_name, f_type, dim)) */
        val pos_dim_map = new HashMap[Int, (String, Int, Int)]()
        val iter = pos_dim.iterator
        while (iter.hasNext) {
          val e = iter.next()
          pos_dim_map.put(e._1._2, (e._1._1, e._1._3, e._2))
        }

        /** ArrayBuffer[(f_index, hash, posInfo)] */
        val pos_map_array = new ArrayBuffer[(Int, Long, PosInfo)]()
        /** Map((f_index, hash), posInfo) */
        val it = pos_map.iterator
        while (it.hasNext) {
          val e = it.next()
          pos_map_array.append((e._1._1, e._1._2, e._2))
        }
        val iterator = pos_map_array.groupBy(s => s._1).toSeq.sortBy(_._1).iterator
        while (iterator.hasNext) {
          val e = iterator.next()
          val f_index = e._1
          val pos_info = e._2.sortWith((a, b) => a._3.pos < b._3.pos)
          val (f_name, f_type, dim) = pos_dim_map(f_index)

          val feature = new JSONObject()
          feature.put("field_name", f_name)
          feature.put("field_index", f_index)
          feature.put("field_type", f_type)
          feature.put("dim", dim)

          val entries = new JSONArray()
          for ((_, hash, stat) <- pos_info) {
            val (exportMean, exportStd) = exportMeanStd(f_type, stat)
            val entry = new JSONObject()
            entry.put("hash", hash)
            entry.put("pos", stat.pos)
            entry.put("sum", stat.sum)
            entry.put("power_sum", stat.powerSum)
            entry.put("count", stat.count)
            entry.put("mean", exportMean)
            entry.put("std", exportStd)
            entries.put(entry)
          }
          feature.put("entries", entries)
          features.put(feature)
        }

        root.put("features", features)
        writer.write(root.toString(2))
        writer.write("\n")
      } finally {
        writer.close()
      }
    } while (false)

    // nn_pos_map.txt
    do {
      val textPath = s"${path.stripSuffix("/")}/nn_pos_map.txt"
      val fs = FileSystem.get(URI.create(textPath), new Configuration())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(textPath), true), "utf-8"))
      try {
        writer.write("field_name,field_index,field_type,dim\n")
        pos_dim.toSeq
          .sortBy { case ((fieldName, fieldIndex, fieldType), _) => (fieldIndex, fieldType, fieldName) }
          .foreach { case ((fieldName, fieldIndex, fieldType), dim) =>
            writer.write(s"${fieldName},${fieldIndex},${fieldType},${dim}\n")
          }
      } finally {
        writer.close()
      }
    } while (false)

    // nn_pos_map.bin
    do {
      val binPath = s"${path.stripSuffix("/")}/nn_pos_map.bin"
      green_println(s"Transformed write pos_map path = ${binPath}")
      val fs = FileSystem.get(URI.create(binPath), new Configuration())
      val writer = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(binPath), true)))
      val pos_field_type_map = new HashMap[Int, Int]()
      pos_dim.iterator.foreach { case ((_, fieldIndex, fieldType), _) =>
        pos_field_type_map.put(fieldIndex, fieldType)
      }
      writeLongLE(writer, yesterday.toLong)
      writeIntLE(writer, pos_map.size)
      /** Map((f_index, hash), (pos, mean, std)) */
      val iterator = pos_map.iterator
      while (iterator.hasNext) {
        // (f_index, hash), (pos, mean, std)
        val kv = iterator.next()
        val fieldType = pos_field_type_map.getOrElse(kv._1._1, FeatureType.Categorical.toInt)
        val (exportMean, exportStd) = exportMeanStd(fieldType, kv._2)
        // f_index
        writeIntLE(writer, kv._1._1)
        // hash
        writeLongLE(writer, kv._1._2)
        // pos
        writeIntLE(writer, kv._2.pos)
        // mean
        writeDoubleLE(writer, exportMean)
        // std
        writeDoubleLE(writer, exportStd)
      }
      writeIntLE(writer, target_map.size)
      val it = target_map.iterator
      while (it.hasNext) {
        val kv = it.next()
        writeIntLE(writer, kv._1)
        writeIntLE(writer, kv._2)
      }
      writer.close()
    } while (false)
    green_println(s"Transformed write pos_map size = ${pos_map.size}")
    green_println(s"Transformed write target_map size = ${target_map.size}")
    green_println(s"Transformed write pos_dim size = ${pos_dim.size}")
  }


  def getMovieInfo(spark: SparkSession, path: String): immutable.Map[Int, (String, Array[String])] = {
    // item_feature.csv
    spark.read
      .option("sep", "\t")
      .csv(s"$path/item_feature")
      .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
      .createOrReplaceTempView("movie_feature")

    val sql =
      s"""
         | select
         |    movie_id,
         |    movie_title,
         |    movie_genres
         | from movie_feature
         | """.stripMargin
    green_println(s"sql:${sql}")

    val app_mapping = spark.sql(sql).rdd
      .flatMap(r => {
        try {
          Some(r.getString(0).toInt -> (r.getString(1), r.getString(2).split("\\|").map(_.trim)))
        } catch {
          case _: Exception => None
        }
      })
      .collectAsMap()
    immutable.HashMap.from(app_mapping)
  }

  /**
   *
   * @param spark
   * @param feature_threshold
   * @param target_threshold
   * @param sample_ratio
   * @param pos_map
   * @param target_map
   * @param pos_dim
   * @param base_dir
   * @return
   * pos_map: HashMap[(f_index, hash), (pos, mean, std)]
   * target_map: HashMap[(target_id, pos)]
   * pos_dim: HashMap[(f_name, f_index, f_type), dim)]
   */
  def run(spark: SparkSession,
          yesterday: String,
          feature_threshold: Int,
          target_threshold: Int,
          sample_ratio: Double,
          pos_map: HashMap[(Int, Long), PosInfo],
          target_map: HashMap[Int, Int],
          pos_dim: HashMap[(String, Int, Int), Int],
          inputDir: String,
          base_dir: String,
          parts: Int
         ): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    val movie_info = getMovieInfo(spark, inputDir)
    green_println(s"movie_info: ${movie_info.size}")

    spark.read
      .option("sep", "\t")
      .csv(s"${inputDir.stripSuffix("/")}/join_sample")
      .toDF("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "movie_feature", "user_behavior")
      .selectExpr("*", "'{}' as user_stat_feature")
      .createOrReplaceTempView("join_sample")

    val sql =
      s"""
         | select * from join_sample
         |""".stripMargin
    green_println(s"Transformed sql=${sql}")

    val trainingSample: RDD[(ML1MTrainSample, Boolean)] = spark.sql(sql).rdd
      .repartition(parts)
      .map(r => {
        val (sample, ret) = ML1MTrainSample.parseSample(r, movie_info)
        (sample, ret)
      })
      .filter(sample => sample._1.target != 0 || ThreadLocalRandom.current().nextDouble() <= sample_ratio)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val ret_counts = trainingSample
      .map(s => (s._2, 1))
      .reduceByKey(_ + _)
      .collect()

    for (ret <- ret_counts) {
      green_println(s"Transformed ret_counts ${ret._1} ${ret._2}")
    }

    val sample_num: Array[(Int, Int)] = trainingSample
      .map(s => s._1)
      .map(sample => (sample.target, 1))
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
    for ((item_id, occurrence) <- sample_num) {
      if (occurrence >= target_threshold && !target_map.contains(item_id)) {
        target_map.put(item_id, index)
        index = index + 1
        newTargetCount += 1
      } else if (target_map.contains(item_id)) {
        existingTargetCount += 1
      } else {
        ignoredTargetCount += 1
      }
      total_number += occurrence
    }
    green_println(s"total valid target number ${total_number}")
    green_println(s"new targets added = ${newTargetCount}")
    green_println(s"existing targets reused = ${existingTargetCount}")
    green_println(s"targets below threshold = ${ignoredTargetCount}")

    /**
     * ArrayBuffer[(f_name, f_index, f_type, hash), (value_sum, value_power_sum, value_count, value_zero, value_nonzero, value_max, value_min)]
     */
    val train_sample_hash_arr = trainingSample
      .map(s => s._1)
      .mapPartitions(samples => {
        val encoder = feature_encoder
        // Map((f_name, f_index, f_type, hash), stat)
        val pos_hash = new mutable.HashMap[(String, Int, Byte, Long), FeatureStat]()
        for (sample <- samples) {
          val one_sample_hash_array = get_hash_info_from_a_sample(sample, encoder)
          for ((f_name, f_index, f_type, f_raw, hash, value) <- one_sample_hash_array) {
            val key = (f_name, f_index, f_type, hash)
            pos_hash.getOrElseUpdate(key, new FeatureStat()).add(value)
          }
        }
        pos_hash.iterator
      })
      .reduceByKey((a, b) => a.merge(b))
      .collect()
    green_println(s"pos_arr.size = ${train_sample_hash_arr.length}")

    var ignoredPosCount = 0
    var reusedHistoryPosCount = 0
    // Map[(f_index, hash), pos]
    val pos_map_local = new HashMap[(Int, Long), Int]
    for (pos_info <- train_sample_hash_arr) {
      val (f_name, f_index, f_type, hash) = pos_info._1
      val fieldKey = (f_name, f_index, f_type.toInt)
      val stat = pos_info._2
      if (stat.count < feature_threshold) {
        pos_map.get((f_index, hash)) match {
          case Some(history) =>
            pos_map_local.put((f_index, hash), history.pos)
            ensureFieldDim(pos_dim, fieldKey, history.pos)
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
            // Continuous feature slots are semantic dimensions. Preserve slot order by forcing hash == pos.
            if (hash <= 0L || hash > Int.MaxValue.toLong) {
              throw new IllegalArgumentException(s"Continuous feature ${f_name}[${f_index}] position must be in [1, ${Int.MaxValue}], got ${hash}")
            }
            hash.toInt
        } else if (pos_map.contains((f_index, hash))) {
          pos_map((f_index, hash)).pos
        } else {
          pos_dim(fieldKey)
        }
        val merged = pos_map.get((f_index, hash)) match {
          case Some(history) => history.copy(pos = pos).merge(stat)
          case None => PosInfo(pos, stat.sum, stat.powerSum, stat.count)
        }
        pos_map.put((f_index, hash), merged)
        pos_map_local.put((f_index, hash), pos)
        ensureFieldDim(pos_dim, fieldKey, pos)
      }
    }
    green_println(s"Transformed ignored_pos_count = ${ignoredPosCount} (本次run()中, 所有特征域内, 被过滤特征值个数)")
    green_println(s"Transformed reused_history_pos_count = ${reusedHistoryPosCount} (本次run()中, 低频但沿用历史词表的特征值个数)")
    green_println(s"Transformed pos_map_local = ${pos_map_local.size} (本次run()中, 所有特征域内, 满足阈值条件的特征值个数)")
    green_println(s"Transformed after pos_map = ${pos_map.size} (累加后的特征值个数)")
    green_println(s"Transformed after target_map = ${target_map.size} (累加后的target个数)")
    green_println(s"Transformed after pos_dim = ${pos_dim.size} (累加后的特征域个数)")

    val tfRecordPath = s"${base_dir.stripSuffix("/")}/${yesterday}/tfrecord"
    val parquetPath = s"${base_dir.stripSuffix("/")}/${yesterday}/parquet"
    val parquetSchema = parquet_schema
    val parquetFieldNames = parquetSchema.fieldNames.toSeq
    val posMapLocalImmutable = immutable.HashMap.from(pos_map_local)
    val targetMapImmutable = immutable.HashMap.from(target_map)
    val parquetRows = trainingSample.map(s => s._1)
      .mapPartitions(samples => {
        val encoder = feature_encoder
        samples.flatMap(sample => {
          val (record, has_feature, has_target) = parse_a_sample_parquet(sample, encoder, posMapLocalImmutable, targetMapImmutable)
          if (has_feature && has_target) {
            Some(Row.fromSeq(record.to_seq(parquetFieldNames)))
          } else {
            None
          }
        })
      })

    spark.createDataFrame(parquetRows, parquetSchema)
      .write
      .mode("overwrite")
      .parquet(parquetPath)

    trainingSample.map(s => s._1)
      .mapPartitions(samples => {
        val encoder = feature_encoder
        samples.flatMap(sample => {
          val (example, has_feature, has_target) = parse_a_sample_tfrecord(sample, encoder, posMapLocalImmutable, targetMapImmutable)
          if (has_feature && has_target) {
            Some(example)
          } else {
            None
          }
        })
      })
      .map(example => {
        val key = new BytesWritable(example.toByteArray)
        val value = NullWritable.get()
        (key, value)
      })
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](tfRecordPath)
    (pos_map, target_map, pos_dim)
  }

  def main(args: Array[String]): Unit = {
    val opts = new Options()
    opts.addOption(null, "feature_threshold", true, "The statistical significance threshold")
    opts.addOption(null, "target_threshold", true, "The statistical significance threshold")
    opts.addOption(null, "sample_ratio", true, "The second sample ratio")
    opts.addOption(null, "input_dir", true, "The base dir of input data")
    opts.addOption(null, "base_dir", true, "The base dir of path")
    opts.addOption(null, "parts", true, "The TrainingNumPartitions")

    val parser = new DefaultParser()
    val cl = parser.parse(opts, args)
    val feature_threshold = cl.getOptionValue("feature_threshold").toInt
    val target_threshold = cl.getOptionValue("target_threshold").toInt
    val sample_ratio = cl.getOptionValue("sample_ratio").toDouble
    val input_dir = cl.getOptionValue("input_dir")
    val base_dir = cl.getOptionValue("base_dir")
    val parts = cl.getOptionValue("parts").toInt
    val yesterday = "20260601"

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    setLogLevel()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    green_println("Hadoop config mapred.output.compress = " + sc.hadoopConfiguration.get("mapred.output.compress"))
    sc.hadoopConfiguration.setBoolean("mapred.output.compress", false)
    green_println("Hadoop config mapred.output.compress = " + sc.hadoopConfiguration.get("mapred.output.compress"))
    for (p <- sc.getConf.getAll) {
      green_println(s"Spark Conf ${p._1} = ${p._2}")
    }

    val path = new Path(base_dir)
    val fs = FileSystem.get(path.toUri, new Configuration())
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
    val outputPath = new Path(s"${base_dir.stripSuffix("/")}/${yesterday}")
    if (fs.exists(outputPath)) {
      green_println(outputPath.toString + " exists and delete.")
      fs.delete(outputPath, true)
    }
    val (pos_map_before, target_map_before, pos_dim_before) = restore_pos_map(base_dir)
    val (pos_map_after, target_map_after, pos_dim_after) = run(
      spark, feature_threshold, target_threshold, sample_ratio,
      pos_map_before, target_map_before, pos_dim_before, input_dir, base_dir, parts
    )
    save_pos_map(base_dir, immutable.HashMap.from(pos_map_after), immutable.HashMap.from(target_map_after), immutable.HashMap.from(pos_dim_after))
    val successPath = new Path(s"${base_dir.stripSuffix("/")}/_SUCCESS")
    if (fs.exists(successPath)) {
      fs.delete(successPath, true)
    }
    fs.create(successPath).close()
  }
}
