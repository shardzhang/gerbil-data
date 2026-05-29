package sample

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author shard zhang
 * @date 2026/5/28 17:45
 * @note 处理 ml-1m 评分数据, 清洗并输出 CSV
 */
object CleanSampleV1 {
  private val INPUT_FILE = "ratings.dat"
  private val OUTPUT_DIR = "raw_sample_clean"
  private val INPUT_SEP = "::"
  private val OUTPUT_SEP = ","
  private val DATE_FORMAT = "yyyy-MM-dd"

  /**
   * 读取并解析原始评分数据
   * @param spark SparkSession
   * @param path 数据根目录
   * @return 清洗后的 DataFrame（user_id, movie_id, rating, time_stamp, day）
   */
  private def getRawSample(spark: SparkSession, path: String): DataFrame = {
    import org.apache.spark.sql.functions._

    val rawSample = spark.read.text(s"${path}/${INPUT_FILE}")
      .withColumn("split_arr", split(col("value"), INPUT_SEP))
      .filter(size(col("split_arr")) === 4)
      .select(
        col("split_arr")(0).as("user_id"),
        col("split_arr")(1).as("movie_id"),
        col("split_arr")(2).cast("long").as("rating"),
        col("split_arr")(3).cast("long").as("time_stamp"),
        from_unixtime(col("split_arr")(3).cast("long"), DATE_FORMAT).as("day")
      )
    rawSample
  }

  def setLogLevel(): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN) // 全局根日志器
    // Logger.getLogger("org").setLevel(Level.WARN)
    // Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    // Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // Logger.getLogger("akka").setLevel(Level.WARN)
    // Logger.getLogger("io.netty").setLevel(Level.WARN)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(s"Usage: ${this.getClass.getSimpleName.stripSuffix("$")} <data_path> <yesterday>")
      System.exit(1)
    }
    val Array(path, yesterday) = args
    println(s"path = $path, yesterday = $yesterday")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN") // org.apache.spark

    try {
      val rawSampleDF = getRawSample(spark, path).cache()
      println(s"rawSampleDF.count() = ${rawSampleDF.count()}")

      val outputPath = new Path(s"$path/$OUTPUT_DIR")
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true)
      }

      rawSampleDF.write
        .mode("overwrite")
        .option("sep", OUTPUT_SEP)
        .option("header", "true")
        .csv(outputPath.toString)
    } finally {
      spark.stop()
    }
  }
}
