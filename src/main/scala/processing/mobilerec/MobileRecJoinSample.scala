package processing.mobilerec

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object MobileRecJoinSample {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: MobileRecJoinSample <path>")
    val path = args(0)
    val savePath = s"$path/join_sample"
    green_println(s"path = $path, save_path = $savePath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println("Spark Version: " + spark.version)

    try {
      spark.read
        .option("sep", SEP)
        .csv(s"$path/clean_sample")
        .toDF("user_id", "item_id", "rating", "time_stamp", "day", "app_category", "review")
        .createOrReplaceTempView("clean_sample")

      spark.read
        .option("sep", SEP)
        .csv(s"$path/item_feature")
        .toDF("app_id", "app_name", "app_category", "content_rating", "num_reviews", "price", "avg_rating", "interact_count", "interact_avg_rate", "hot_rank")
        .createOrReplaceTempView("item_feature")

      spark.read
        .option("sep", SEP)
        .csv(s"$path/user_app_rate_sequence")
        .toDF("user_id", "feature")
        .createOrReplaceTempView("user_app_rate")

      val sql =
        s"""
           |, item_feature_json as (
           |  select
           |    app_id,
           |    to_json(struct(app_id, app_name, app_category, content_rating, num_reviews, price, avg_rating)) as feature
           |  from item_feature
           |)
           |
           |select
           |  s.user_id,
           |  s.item_id,
           |  s.time_stamp,
           |  s.rating,
           |  s.day,
           |  s.app_category,
           |  s.review,
           |  to_json(
           |    named_struct(
           |      'user_app_rate', coalesce(r.feature, '')
           |    )
           |  ) as user_behavior,
           |  coalesce(f.feature, '{}') as item_feature
           |from clean_sample s
           |left join user_app_rate r
           |  on s.user_id = r.user_id
           |left join item_feature_json f
           |  on s.item_id = f.app_id
           |""".stripMargin

      val joinSample = spark.sql(sql).cache()
      green_println(f"joinSample.count(): ${joinSample.count()}")
      DataQualityChecker.check(joinSample, "join_sample", savePath)
      joinSample.show()
      joinSample.printSchema()

      joinSample
        .selectExpr("user_id", "item_id", "time_stamp", "rating", "day", "app_category", "review", "user_behavior", "item_feature")
        .write
        .mode("overwrite")
        .option("sep", SEP)
        .csv(savePath)
      joinSample.unpersist()
    } finally {
      spark.stop()
    }
  }
}
