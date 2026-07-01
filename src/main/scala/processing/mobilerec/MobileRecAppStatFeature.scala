package processing.mobilerec

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object MobileRecAppStatFeature {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: MobileRecAppStatFeature <path>")
    val path = args(0)
    val outputPath = s"$path/item_feature"
    green_println(s"path = $path, outputPath: $outputPath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"$path/app_meta/app_meta.csv")
        .createOrReplaceTempView("app_meta")

      spark.read
        .option("sep", SEP)
        .csv(s"$path/clean_sample")
        .toDF("user_id", "item_id", "rating", "time_stamp", "day", "app_category", "review")
        .createOrReplaceTempView("clean_sample")

      val sql =
        s"""
           |select
           |  a.app_package as app_id,
           |  a.app_name,
           |  a.app_category,
           |  a.content_rating,
           |  a.num_reviews,
           |  a.price,
           |  a.avg_rating,
           |  coalesce(s.movie_rate_count, 0) as interact_count,
           |  coalesce(s.movie_avg_rate, 0.0) as interact_avg_rate,
           |  row_number() over (order by coalesce(s.movie_rate_count, 0) desc) as hot_rank
           |from app_meta a
           |left join (
           |  select
           |    item_id,
           |    count(distinct user_id) as movie_rate_count,
           |    round(avg(cast(rating as double)), 2) as movie_avg_rate
           |  from clean_sample
           |  group by item_id
           |) s on a.app_package = s.item_id
           |""".stripMargin

      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      DataQualityChecker.check(result, "item_feature", outputPath)
      result.show()
      result.printSchema()

      result
        .select("app_id", "app_name", "app_category", "content_rating", "num_reviews", "price", "avg_rating", "interact_count", "interact_avg_rate", "hot_rank")
        .write
        .mode("overwrite")
        .option("sep", SEP)
        .csv(outputPath)
      result.unpersist()
    } finally {
      spark.stop()
    }
  }
}
