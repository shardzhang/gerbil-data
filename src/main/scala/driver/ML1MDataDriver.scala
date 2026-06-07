package driver

import org.apache.spark.sql.types._
import org.tensorflow.example.Example
import sample.ML1MTrainSample
import encoder.FeatureEncoder4ML1M
import encoder.vectorizer.FeatureEncoder
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

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, DataInputStream, DataOutputStream, InputStreamReader, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.ThreadLocalRandom
import scala.collection.compat.MapFactoryExtensionMethods
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * @author shard zhang
 * @date 2026/6/5 11:38
 * @note
 * 基于TFRecord的样本生成, 包括:
 *
 *      1. TFRecord文件
 *      2. nn_pos.txt编码表(用于离线模型训练)
 *      3. nn_pos.bin编码表(用于在线模型推理)
 */
final class FeatureStats(
                          var sum: Double = 0.0,
                          var powerSum: Double = 0.0,
                          var count: Long = 0L
                        ) extends Serializable {
  def add(value: Float): FeatureStats = {
    val valueDouble = value.toDouble
    sum += valueDouble
    powerSum += valueDouble * valueDouble
    count += 1L
    this
  }

  def merge(other: FeatureStats): FeatureStats = {
    sum += other.sum
    powerSum += other.powerSum
    count += other.count
    this
  }
}


object ML1MDataDriver extends Serializable {
  val max_dim: Long = 1L << 60
  private val TrainingNumPartitions = 64
  private val StatsNumPartitions = 64

  private val OutputDay = "20260601"

  private def normalizeDir(path: String): String = path.stripSuffix("/")

  private def posMapTextPath(baseDir: String): String = s"${normalizeDir(baseDir)}/nn_pos_map.txt"

  private def posMapBinPath(baseDir: String): String = s"${normalizeDir(baseDir)}/nn_pos_map.bin"

  private def readIntLE(reader: DataInputStream): Int = Integer.reverseBytes(reader.readInt())

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
  def get_pos_info_from_a_sample(sample: ML1MTrainSample,
                                 encoder: FeatureEncoder[ML1MTrainSample]): ListBuffer[(String, Int, Byte, String, Long, Float)] = {
    encoder.get_pos_info(sample, max_dim)
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
    val fields = new ListBuffer[StructField]()
    fields.append(StructField("target", FloatType, nullable = true))
    for ((f_name, _) <- feature_encoder.get_field_name_and_index()) {
      fields.append(StructField(f_name + "_index", ArrayType(LongType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_value", ArrayType(FloatType, containsNull = false), nullable = true))
    }
    StructType(fields.toSeq)
  }

  def restore_pos_map(path: String): (
    HashMap[(Int, Long), (Int, Double, Double)],
      HashMap[Int, Int],
      HashMap[(String, Int, Int), Int]
    ) = {

    /** Map((f_name, f_index, f_type), dim) */
    val pos_dim_map = new mutable.HashMap[(String, Int, Int), Int]()
    /** Map((f_index, pos), (index, mean, std)) */
    val pos_map = new mutable.HashMap[(Int, Long), (Int, Double, Double)]()
    /** Map(target, index) */
    val target_map = new mutable.HashMap[Int, Int]()

    // nn_pos_map.txt
    try {
      val textPath = posMapTextPath(path)
      val fs = FileSystem.get(URI.create(textPath), new Configuration())
      val reader = new BufferedReader(new InputStreamReader(fs.open(new Path(textPath)), "utf-8"))
      var line = reader.readLine()
      green_println(f"line: ${line}")
      line = reader.readLine()
      while (line != null) {
        val items = line.split(",")
        pos_dim_map.put((items(0), items(1).toInt, items(2).toInt), items(3).toInt)
        line = reader.readLine()
      }
      reader.close()
    } catch {
      case e: Throwable => green_println(e.toString)
    }

    // nn_pos_map.bin
    try {
      val binPath = posMapBinPath(path)
      val fs = FileSystem.get(URI.create(binPath), new Configuration())
      val reader = new DataInputStream(new BufferedInputStream(fs.open(new Path(binPath))))
      val timestamp = readLongLE(reader)
      var size = readIntLE(reader)
      green_println(s"timestamp: ${timestamp}")
      green_println(s"pos_map size: ${size}")
      // index不保序. 即不是从0开始一致增大,可能是乱序的, 但是pos和index一定是正确对应的
      while (size > 0) {
        pos_map.put(
          (readIntLE(reader), readLongLE(reader)), (readIntLE(reader), readDoubleLE(reader), readDoubleLE(reader))
        )
        size = size - 1
      }
      size = readIntLE(reader)
      green_println(s"target_map size = ${size}")
      while (size > 0) {
        target_map.put(readIntLE(reader), readIntLE(reader))
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
                   pos_map: immutable.HashMap[(Int, Long), (Int, Double, Double)],
                   target_map: immutable.HashMap[Int, Int],
                   pos_dim: immutable.HashMap[(String, Int, Int), Int]): Unit = {
    // nn_pos_map.txt
    do {
      val textPath = posMapTextPath(path)
      val fs = FileSystem.get(URI.create(textPath), new Configuration())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(textPath), true), "utf-8"))
      writer.write("target" + "," + target_map.size + "\n")

      /** Map(f_index, (f_name, f_type, dim)) */
      val pos_dim_map = new HashMap[Int, (String, Int, Int)]()
      val iter = pos_dim.iterator
      while (iter.hasNext) {
        val e = iter.next()
        pos_dim_map.put(e._1._2, (e._1._1, e._1._3, e._2))
      }

      /** ListBuffer[(f_index, index, mean, std)] */
      val pos_map_array = new ListBuffer[(Int, Int, Double, Double)]()
      /** Map((f_index, pos), (index, mean, std)) */
      val it = pos_map.iterator
      while (it.hasNext) {
        val e = it.next()
        // (f_index, index, mean, std)
        pos_map_array.append((e._1._1, e._2._1, e._2._2, e._2._3))
      }
      // Map (f_index -> ListBuffer((f_index, index, mean, std),...))
      val iterator = pos_map_array.groupBy(s => s._1).iterator
      while (iterator.hasNext) {
        // f_index -> ListBuffer((f_index, index, mean, std),...)
        val e = iterator.next()
        val f_index = e._1
        // Sort with index in same f_index.
        val pos_info = e._2.sortWith((a, b) => a._2 < b._2)
        // (f_name, f_type, dim)
        val (f_name, f_type, dim) = pos_dim_map(f_index)
        // f_name, f_index, f_type, dim, size
        writer.write(f_name + "," + f_index + "," + f_type + "," + dim + "," + pos_info.size)
        for ((_, index, mean, std) <- pos_info) {
          writer.write("," + index + ":" + mean + ":" + std)
        }
        writer.write("\n")
      }
      writer.close()
    } while (false)

    // nn_pos_map.bin
    do {
      val binPath = posMapBinPath(path)
      green_println(s"Transformed write pos_map path = ${binPath}")
      val fs = FileSystem.get(URI.create(binPath), new Configuration())
      val writer = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(binPath), true)))
      writeLongLE(writer, OutputDay.toLong)
      writeIntLE(writer, pos_map.size)
      /** Map((f_index, pos), (index, mean, std)) */
      val iterator = pos_map.iterator
      while (iterator.hasNext) {
        // (f_index, pos), (index, mean, std)
        val kv = iterator.next()
        // f_index
        writeIntLE(writer, kv._1._1)
        // pos
        writeLongLE(writer, kv._1._2)
        // index
        writeIntLE(writer, kv._2._1)
        // mean
        writeDoubleLE(writer, kv._2._2)
        // std
        writeDoubleLE(writer, kv._2._3)
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


  def getMovieInfo(spark: SparkSession, path: String): immutable.Map[Int, (String, Array[String])] =  {
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
   * pos_dim: HashMap[(f_name, f_index, f_type), dim)]
   * pos_map: HashMap[(f_index, pos), (index, mean, std)]
   * target_map: HashMap[(target_id, pos)]
   */
  def run(spark: SparkSession,
          feature_threshold: Int,
          target_threshold: Int,
          sample_ratio: Double,
          pos_map: HashMap[(Int, Long), (Int, Double, Double)],
          target_map: HashMap[Int, Int],
          pos_dim: HashMap[(String, Int, Int), Int],
          inputDir: String,
          base_dir: String
         ): (HashMap[(Int, Long), (Int, Double, Double)], HashMap[Int, Int], HashMap[(String, Int, Int), Int]) = {

    val movie_info = getMovieInfo(spark, inputDir)
    green_println(s"movie_info: ${movie_info.size}")

    spark.read
      .option("sep", "\t")
      .csv(s"${normalizeDir(inputDir)}/join_sample")
      .toDF("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "movie_feature", "user_behavior")
      .selectExpr("*", "'{}' as user_stat_feature")
      .createOrReplaceTempView("join_sample")

    val sql =
      s"""
         | select * from join_sample
         |""".stripMargin
    green_println(s"Transformed sql=${sql}")
    import spark.implicits._

    val trainingSample: RDD[(ML1MTrainSample, Boolean)] = spark.sql(sql).rdd.repartition(TrainingNumPartitions).map(r => {
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
     * ListBuffer[(f_name, f_index, f_type, pos, fea), (value_sum, value_power_sum, value_count, value_zero, value_nonzero, value_max, value_min)]
     */
    val train_sample_pos_arr = trainingSample
      .map(s => s._1)
      .mapPartitions(samples => {
        val encoder = feature_encoder
        // Map[(f_name, f_index, f_type, pos, index), (sum, power_sum, count, zero, nonzero, max, min)]
        val pos_hash = new mutable.HashMap[(String, Int, Byte, Long), FeatureStats]()
        for (sample <- samples) {
          val one_sample_pos_array = get_pos_info_from_a_sample(sample, encoder)
          for ((f_name, f_index, f_type, f_raw, pos, value) <- one_sample_pos_array) {
            val key = (f_name, f_index, f_type, pos)
            pos_hash.getOrElseUpdate(key, new FeatureStats()).add(value)
          }
        }
        pos_hash.iterator
      })
      .reduceByKey(
        (a, b) => a.merge(b),
        StatsNumPartitions
      )
      .collect()
    green_println(s"pos_arr.size = ${train_sample_pos_arr.length}")

    var ignoredPosCount = 0
    val pos_map_local = new HashMap[(Int, Long), Int]
    for (pos_info <- train_sample_pos_arr) {
      val (f_name, f_index, f_type, pos) = pos_info._1
      val stat = pos_info._2
      // variance. D(x) = E(x^2) - E^2(x), E表示期望
      // power_sum / count - power(sum / cnt, 2)
      val variance = stat.powerSum / total_number - Math.pow(stat.sum / total_number, 2)
      var mean = stat.sum / total_number
      var std = math.sqrt(variance + 0.000001)
      if (f_type == 1) {
        mean = 0D
        std = 1D
      }
      if (stat.count < feature_threshold) {
        ignoredPosCount += 1
      } else {
        if (!pos_dim.contains((f_name, f_index, f_type))) {
          pos_dim.put((f_name, f_index, f_type), 1)
          pos_map.put((f_index, 0L), (0, 0D, 1.0D))
        }
        if (!pos_map.contains((f_index, pos))) {
          val index = pos_dim((f_name, f_index, f_type))
          pos_map.put((f_index, pos), (index, mean, std))
          pos_map_local.put((f_index, pos), index)
          pos_dim.put((f_name, f_index, f_type), index + 1)
        } else {
          val (index, history_mean, history_std) = pos_map((f_index, pos))
          pos_map.put((f_index, pos), (index, (mean + history_mean) / 2.0, (std + history_std) / 2.0))
          pos_map_local.put((f_index, pos), index)
        }
      }
    }
    green_println(s"Transformed ignored_pos_count = ${ignoredPosCount} (本次run()中, 所有特征域内, 被过滤特征值个数)")
    green_println(s"Transformed pos_map_local = ${pos_map_local.size} (本次run()中, 所有特征域内, 满足阈值条件的特征值个数)")
    green_println(s"Transformed after pos_map = ${pos_map.size} (累加后的特征值个数)")
    green_println(s"Transformed after target_map = ${target_map.size} (累加后的target个数)")
    green_println(s"Transformed after pos_dim = ${pos_dim.size} (累加后的特征域个数)")

    val tfRecordPath = s"${normalizeDir(base_dir)}/${OutputDay}/tfrecord"
    val parquetPath = s"${normalizeDir(base_dir)}/${OutputDay}/parquet"
    val parquetSchema = parquet_schema
    val parquetFieldNames = parquetSchema.fieldNames.toSeq
    val posMapLocalImmutable = immutable.HashMap.from(pos_map_local)
    val targetMapImmutable = immutable.HashMap.from(target_map)
    val parquetRows = trainingSample.map(s => s._1)
      .mapPartitions(samples => {
        val encoder = feature_encoder
        samples.flatMap(sample => {
          val (record, _, has_target) = parse_a_sample_parquet(sample, encoder, posMapLocalImmutable, targetMapImmutable)
          if (has_target) {
            Some(Row.fromSeq(record.to_seq(parquetFieldNames)))
          } else {
            None
          }
        })
      })

    spark.createDataFrame(parquetRows, parquetSchema)
      .write
      .mode("overwrite") // 覆盖写入
      .parquet(parquetPath) // 写入路径

    trainingSample.map(s => s._1)
      .mapPartitions(samples => {
        val encoder = feature_encoder
        samples.flatMap(sample => {
          val (example, has_feature, _) = parse_a_sample_tfrecord(sample, encoder, posMapLocalImmutable, targetMapImmutable)
          if (has_feature) {
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

    val parser = new DefaultParser()
    val cl = parser.parse(opts, args)
    val feature_threshold = cl.getOptionValue("feature_threshold").toInt
    val target_threshold = cl.getOptionValue("target_threshold").toInt
    val sample_ratio = cl.getOptionValue("sample_ratio").toDouble
    val input_dir = cl.getOptionValue("input_dir")
    val base_dir = cl.getOptionValue("base_dir")

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
    val outputPath = new Path(s"${normalizeDir(base_dir)}/${OutputDay}")
    if (fs.exists(outputPath)) {
      green_println(outputPath.toString + " exists and delete.")
      fs.delete(outputPath, true)
    }
    val (pos_map_before, target_map_before, pos_dim_before) = restore_pos_map(base_dir)
    val (pos_map_after, target_map_after, pos_dim_after) = run(
      spark, feature_threshold, target_threshold, sample_ratio,
      pos_map_before, target_map_before, pos_dim_before, input_dir, base_dir
    )
    save_pos_map(base_dir, immutable.HashMap.from(pos_map_after), immutable.HashMap.from(target_map_after), immutable.HashMap.from(pos_dim_after))
    val successPath = new Path(s"${normalizeDir(base_dir)}/_SUCCESS")
    if (fs.exists(successPath)) {
      fs.delete(successPath, true)
    }
    fs.create(successPath).close()
  }
}
