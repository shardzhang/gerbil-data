package processing.alictr

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object AliCtrCleanSample {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: AliCtrCleanSample <path>")
    val path = args(0)
    val outputPath = s"$path/clean_sample"
    green_println(s"path = $path, outputPath: $outputPath")
    setLogLevel()
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      spark.read.option("header", "true").option("inferSchema", "true").csv(s"$path/raw_sample.csv")
        .createOrReplaceTempView("raw_sample")
      val sql =
        """select cast(user as string) as user_id, cast(adgroup_id as string) as item_id,
          |  cast(clk as int) as clk, cast(nonclk as int) as nonclk,
          |  cast(time_stamp as bigint) as time_stamp, pid
          |from raw_sample
          |where user is not null and adgroup_id is not null and clk is not null and time_stamp is not null""".stripMargin
      val cleanSample = spark.sql(sql).cache()
      green_println(s"cleanSample.count() = ${cleanSample.count()}")
      DataQualityChecker.check(cleanSample, "clean_sample", outputPath)
      cleanSample.selectExpr("user_id", "item_id", "clk", "nonclk", "time_stamp", "pid")
        .write.mode("overwrite").option("sep", SEP).csv(outputPath)
      cleanSample.unpersist()
    } finally { spark.stop() }
  }
}
