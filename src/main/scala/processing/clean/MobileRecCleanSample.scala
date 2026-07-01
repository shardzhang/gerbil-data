package processing.clean

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object MobileRecCleanSample {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: MobileRecCleanSample <path>")
    val path = args(0)
    val outputPath = s"$path/clean_sample"
    green_println(s"path = $path, outputPath: $outputPath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      // app_package,review,rating,votes,date,uid,formated_date,unix_timestamp,app_category
      // com.cleverapps.heroes,It's really a fun game,5,1,"October 21, 2018",shqoc6X1fcJRLEmx,2018-10-21,1540094400.0,Casual
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"$path/interactions/mobilerec_final.csv")
        .createOrReplaceTempView("interactions_raw")

      val sql =
        s"""
           | select
           |    cast(uid as string) as user_id,
           |    cast(app_package as string) as item_id,
           |    cast(rating as int) as rating,
           |    cast(votes as int) as votes,
           |    cast(unix_timestamp as bigint) as time_stamp,
           |    formated_date as day,
           |    app_category as category,
           |    cast(review as string) as review
           | from interactions_raw
           | where uid is not null and length(uid) > 0
           | and app_package is not null and length(app_package) > 0
           | and rating is not null and rating >= 1 and rating <= 5
           | and unix_timestamp is not null
           |""".stripMargin
      green_println(f"sql: ${sql}")

      val cleanSample = spark.sql(sql).cache()
      green_println(s"cleanSample.count() = ${cleanSample.count()}")

      DataQualityChecker.check(cleanSample, "clean_sample", outputPath)
      cleanSample.show()
      cleanSample.printSchema()

      cleanSample
        .selectExpr("user_id", "item_id", "rating", "votes", "time_stamp", "day", "category", "review")
        .write
        .mode("overwrite")
        .option("sep", SEP)
        .csv(outputPath)
      cleanSample.unpersist()
    } finally {
      spark.stop()
    }
  }
}
