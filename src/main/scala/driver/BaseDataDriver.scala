package driver

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import com.google.common.io.{LittleEndianDataInputStream, LittleEndianDataOutputStream}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.tensorflow.example.Example
import org.json.{JSONArray, JSONObject}

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag
import utils.LogUtils.green_println
import utils.ParquetRecord
import encoder.vectorizer.FeatureEncoder
import encoder.vectorizer.FeatureType

/** Feature value statistics: sum, sum of squares, and count. */
final class FeatureStat(var sum: Double = 0.0D, var powerSum: Double = 0.0D, var count: Long = 0L) extends Serializable {
  def add(value: Float): FeatureStat = {
    sum += value.toDouble
    powerSum += value.toDouble * value.toDouble
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

/** Encoded position info for a feature value: pos in field, sum, powerSum, count. */
final case class PosInfo(pos: Int, sum: Double = 0.0D, powerSum: Double = 0.0D, count: Long = 0L) extends Serializable {
  // mean. E(x) = sum / count
  def mean: Double = {
    if (count <= 0L) {
      return 0.0D
    }
    sum / count.toDouble
  }

  // variance. D(x) = E(x^2) - E^2(x) = power_sum / count - power(sum / count, 2)
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

/** Abstract driver for loading samples, computing feature statistics, and writing TFRecord/Parquet output. */
abstract class BaseDataDriver[T: ClassTag] extends Serializable {
  def max_dim: Long

  def feature_encoder: FeatureEncoder[T]

  var hadoopConf: Configuration = hadoopConf

  /** Load and parse training samples from input directory. */
  def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(T, Boolean)]

  /** Extract the target label from a sample. */
  def getSampleTarget(sample: T): Int

  /** Decide whether to keep a sample. Keeps all non-zero targets; others sampled by ratio. */
  def keepSample(sample: T, sample_ratio: Double): Boolean = {
    getSampleTarget(sample) != 0 || ThreadLocalRandom.current().nextDouble() <= sample_ratio
  }

  /** Extract hash info tuples from a single sample: (f_name, f_index, f_type, format, hash, value). */
  def getHashInfo(sample: T, encoder: FeatureEncoder[T]): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.get_hash_info(sample, max_dim)
  }

  /** Parse a sample into TFRecord format using the encoder and position maps. */
  def parseTfrecord(sample: T,
                    encoder: FeatureEncoder[T],
                    pos_map: collection.Map[(Int, Long), Int],
                    target_map: collection.Map[Int, Int]): (Example, Boolean, Boolean) = {
    val builder = Example.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }

  /** Parse a sample into Parquet format using the encoder and position maps. */
  def parseParquet(sample: T,
                   encoder: FeatureEncoder[T],
                   pos_map: collection.Map[(Int, Long), Int],
                   target_map: collection.Map[Int, Int]): (ParquetRecord, Boolean, Boolean) = {
    val builder = ParquetRecord.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }

  /** Build the Parquet schema: target field + raw/index/value arrays per feature. */
  def parquet_schema: StructType = {
    val fields = new ArrayBuffer[StructField]()
    fields.append(StructField("target", FloatType, nullable = true))
    for ((f_name, _) <- feature_encoder.get_field_name_and_index()) {
      fields.append(StructField(f_name + "_raw", ArrayType(StringType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_index", ArrayType(LongType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_value", ArrayType(FloatType, containsNull = false), nullable = true))
    }
    StructType(fields)
  }

  /** Reconstruct PosInfo from legacy mean/std-only encoding table format. */
  protected def legacyPosInfo(pos: Int, mean: Double, std: Double, count: Long): PosInfo = {
    val safeCount = math.max(count, 1L)
    val variance = math.max(std * std - 0.000001D, 0.0D)
    val sum = mean * safeCount.toDouble
    val powerSum = (variance + mean * mean) * safeCount.toDouble
    PosInfo(pos, sum, powerSum, safeCount)
  }

  /** Read the full content of a text file via BufferedReader. */
  protected def readText(reader: BufferedReader): String = {
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

  /**
   * Restore pos_map, target_map, and pos_dim_map from a JSON file at the given path.
   *
   * @param posMap   target map: (f_index, hash) -> PosInfo
   * @param targetMap target map: target_id -> encoded pos
   * @param posDimMap dimension map: (f_name, f_index, f_type) -> dim
   * @return true if the JSON file exists and was restored successfully
   */
  protected def restoreFromJson(path: String,
                                yesterday: String,
                                posMap: mutable.HashMap[(Int, Long), PosInfo],
                                targetMap: mutable.HashMap[Int, Int],
                                posDimMap: mutable.HashMap[(String, Int, Int), Int]): Boolean = {
    val jsonPath = s"${path}/${yesterday}/pos_map.json"
    val fs = FileSystem.get(URI.create(jsonPath), hadoopConf)
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
          val posInfo = PosInfo(
            pos = entry.getInt("pos"),
            sum = entry.getDouble("sum"),
            powerSum = entry.getDouble("power_sum"),
            count = entry.getLong("count")
          )
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

  /** Restore field dimension info only from a legacy text-format pos_map file. */
  protected def restoreFromText(path: String,
                                yesterday: String,
                                posDimMap: mutable.HashMap[(String, Int, Int), Int]): Boolean = {
    val textPath = s"${path}/${yesterday}/pos_map.txt"
    val fs = FileSystem.get(URI.create(textPath), hadoopConf)
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

  /**
   * Restore from binary format (legacy). Primarily kept for compatibility or online serving.
   * Default restore path uses JSON.
   */
  protected def restoreFromBin(path: String,
                               yesterday: String,
                               posMap: mutable.HashMap[(Int, Long), PosInfo],
                               targetMap: mutable.HashMap[Int, Int],
                               posDimMap: mutable.HashMap[(String, Int, Int), Int]): Unit = {
    val binPath = s"${path}/${yesterday}/pos_map.bin"
    try {
      val fs = FileSystem.get(URI.create(binPath), hadoopConf)
      val reader = new LittleEndianDataInputStream(new BufferedInputStream(fs.open(new Path(binPath))))
      try {
        val timestamp = reader.readLong()
        var size = reader.readInt()
        green_println(s"timestamp: ${timestamp}")
        green_println(s"pos_map size: ${size}")

        while (size > 0) {
          val f_name = reader.readUTF()
          val f_index = reader.readInt()
          val f_type = reader.readInt()
          val dim = reader.readInt()
          val hash = reader.readLong()
          val pos = reader.readInt()
          val mean = reader.readDouble()
          val std = reader.readDouble()
          posMap.put((f_index, hash), legacyPosInfo(pos, mean, std, 1L))
          posDimMap.put((f_name, f_index, f_type), dim)
          size = size - 1
        }

        size = reader.readInt()
        green_println(s"target_map size = ${size}")
        while (size > 0) {
          val target_id = reader.readInt()
          val pos = reader.readInt()
          if (!targetMap.contains(target_id)) {
            targetMap.put(target_id, pos)
          }
          size = size - 1
        }
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception =>
        green_println(s"restoreFromBin failed for ${binPath}: ${e.toString}")
    }
  }

  /**
   * Restore all encoding maps from disk:
   * - pos_map:    (f_index, hash) -> PosInfo
   * - target_map: target_id -> encoded pos
   * - pos_dim:    (f_name, f_index, f_type) -> field dimension
   */
  def restore_pos_map(path: String, yesterday: String): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {
    val pos_dim_map = new mutable.HashMap[(String, Int, Int), Int]()
    val pos_map = new mutable.HashMap[(Int, Long), PosInfo]()
    val target_map = new mutable.HashMap[Int, Int]()

    restoreFromJson(path, yesterday, pos_map, target_map, pos_dim_map)

    green_println(s"read pos_map size = ${pos_map.size}")
    green_println(s"read target_map size = ${target_map.size}")
    green_println(s"read pos_dim_map size = ${pos_dim_map.size}")
    (pos_map, target_map, pos_dim_map)
  }

  /**
   * Save all encoding maps in three formats:
   * - pos_map.json: for offline model training
   * - pos_map.txt:  human-readable text (debugging)
   * - pos_map.bin:  compact binary for online serving
   */
  def save_pos_map(path: String,
                   yesterday: String,
                   pos_map: collection.Map[(Int, Long), PosInfo],
                   target_map: collection.Map[Int, Int],
                   pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    saveToJson(path, yesterday, pos_map, target_map, pos_dim)
    saveToText(path, yesterday, pos_dim)
    saveToBin(path, yesterday, pos_map, target_map, pos_dim)
    green_println(s"write pos_map size: ${pos_map.size}")
    green_println(s"write target_map size: ${target_map.size}")
    green_println(s"write pos_dim size: ${pos_dim.size}")
  }

  /**
   * Main pipeline:
   * 1. Load and filter training samples
   * 2. Build/update target_map
   * 3. Build/update pos_map and pos_dim via hash aggregation
   * 4. Write output in Parquet and TFRecord formats
   */
  def run(spark: org.apache.spark.sql.SparkSession,
          yesterday: String,
          feature_threshold: Int,
          target_threshold: Int,
          sample_ratio: Double,
          pos_map: HashMap[(Int, Long), PosInfo],
          target_map: HashMap[Int, Int],
          pos_dim: HashMap[(String, Int, Int), Int],
          input_dir: String,
          output_dir: String,
          parts: Int): (HashMap[(Int, Long), PosInfo], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    val trainingSample = loadTrainingSamples(spark, input_dir, parts)
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
        val pos_hash = new mutable.HashMap[(String, Int, Byte, Long), FeatureStat]()
        for (sample <- samples) {
          val one_sample_hash_array = getHashInfo(sample, encoder)
          for ((f_name, f_index, f_type, _, hash, value) <- one_sample_hash_array) {
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

    val parquetRows: RDD[Row] = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        val encoder = feature_encoder
        samples.flatMap(sample => {
          val (record, has_feature, has_target) = parseParquet(sample, encoder, posMapLocalImmutable, targetMapImmutable)
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

    trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        val encoder = feature_encoder
        samples.flatMap(sample => {
          val (example, has_feature, has_target) = parseTfrecord(sample, encoder, posMapLocalImmutable, targetMapImmutable)
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

  /** Save pos_map and target_map in compact binary format (pos_map.bin). */
  protected def saveToBin(path: String,
                          yesterday: String,
                          pos_map: collection.Map[(Int, Long), PosInfo],
                          target_map: collection.Map[Int, Int],
                          pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    do {
      val binPath = s"${path}/${yesterday}/pos_map.bin"
      green_println(s"write pos_map.bin path = ${binPath}")
      val fs = FileSystem.get(URI.create(binPath), hadoopConf)
      val writer = new LittleEndianDataOutputStream(new BufferedOutputStream(fs.create(new Path(binPath), true)))

      val pos_dim_map = new mutable.HashMap[Int, (String, Int, Int)]()
      val iter = pos_dim.iterator
      while (iter.hasNext) {
        val e = iter.next()
        pos_dim_map.put(e._1._2, (e._1._1, e._1._3, e._2))
      }

      writer.writeLong(yesterday.replaceAll("-", "").toLong)
      writer.writeInt(pos_map.size)

      /** Map((f_index, hash), (pos, mean, std)) */
      val iterator = pos_map.iterator
      while (iterator.hasNext) {
        // (f_index, hash), (pos, mean, std)
        val kv = iterator.next()
        val (f_name, f_type, dim) = pos_dim_map(kv._1._1)
        val (mean, std) = {
          if (f_type == FeatureType.Categorical) {
            (0.0D, 1.0D)
          } else {
            (kv._2.mean, kv._2.std)
          }
        }

        writer.writeUTF(f_name)
        writer.writeInt(kv._1._1)
        writer.writeInt(f_type)
        writer.writeInt(dim)
        writer.writeLong(kv._1._2)
        writer.writeInt(kv._2.pos)
        writer.writeDouble(mean)
        writer.writeDouble(std)
      }

      writer.writeInt(target_map.size)
      val it = target_map.iterator
      while (it.hasNext) {
        val kv = it.next()
        writer.writeInt(kv._1)
        writer.writeInt(kv._2)
      }
      writer.close()
    } while (false)
  }

  /** Save field dimension info as human-readable CSV (pos_map.txt). */
  protected def saveToText(path: String,
                           yesterday: String,
                           pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    do {
      val textPath = s"${path}/${yesterday}/pos_map.txt"
      green_println(s"write pos_map.text path = ${textPath}")
      val fs = FileSystem.get(URI.create(textPath), hadoopConf)
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
  }

  /** Save full encoding table as JSON (pos_map.json) for offline model training. */
  protected def saveToJson(path: String,
                           yesterday: String,
                           pos_map: collection.Map[(Int, Long), PosInfo],
                           target_map: collection.Map[Int, Int],
                           pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    do {
      val jsonPath = s"${path}/${yesterday}/pos_map.json"
      green_println(s"write pos_map.json path = ${jsonPath}")
      val fs = FileSystem.get(URI.create(jsonPath), hadoopConf)
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(jsonPath), true), "utf-8"))

      try {
        val root = new JSONObject()
        // Record the date stamp
        root.put("yesterday", yesterday)

        val features = new JSONArray()
        // Map: f_index -> (f_name, f_type, dim)
        val pos_dim_map = new mutable.HashMap[Int, (String, Int, Int)]()
        val iter = pos_dim.iterator
        while (iter.hasNext) {
          val e = iter.next()
          pos_dim_map.put(e._1._2, (e._1._1, e._1._3, e._2))
        }

        // Collect all (f_index, hash, PosInfo) entries into an array
        val pos_map_array = new ArrayBuffer[(Int, Long, PosInfo)]()
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
            val entry = new JSONObject()
            val (mean, std) = {
              if (f_type == FeatureType.Categorical) {
                (0.0D, 1.0D)
              } else {
                (stat.mean, stat.std)
              }
            }
            entry.put("hash", hash)
            entry.put("pos", stat.pos)
            entry.put("sum", stat.sum)
            entry.put("power_sum", stat.powerSum)
            entry.put("count", stat.count)
            entry.put("mean", mean)
            entry.put("std", std)
            entries.put(entry)
          }
          feature.put("entries", entries)
          features.put(feature)
        }
        root.put("features", features)
        root.put("feature_size", features.length())
        root.put("target_size", target_map.size)

        // Save target_id -> encoded_pos mapping
        val targets = new JSONObject()
        target_map.toSeq.sortBy(_._2)
          .foreach { case (target_id, pos) =>
            targets.put(target_id.toString, pos)
          }
        root.put("targets", targets)

        writer.write(root.toString(2))
        writer.write("\n")
      } finally {
        writer.close()
      }
    } while (false)
  }
}
