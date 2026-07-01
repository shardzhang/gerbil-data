package processing.feature

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object MobileRecUserBehaviorSequence {
  private val TOP_N = 200
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: MobileRecUserBehaviorSequence <path>")
    val path = args(0)
    val outputPath = s"$path/user_app_rate_sequence"
    green_println(s"path = $path, outputPath: $outputPath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      spark.read
        .option("sep", SEP)
        .csv(s"$path/clean_sample")
        .toDF("user_id", "item_id", "rating", "time_stamp", "day", "app_category", "review")
        .createOrReplaceTempView("clean_sample")

      val sql =
        s"""
           |with latest_per_user_item_ranked as (
           |  select
           |    user_id,
           |    item_id,
           |    cast(rating as int) as rating,
           |    cast(time_stamp as bigint) as timestamp
           |  from clean_sample
           |  where user_id is not null and length(user_id) > 0
           |    and item_id is not null and length(item_id) > 0
           |    and rating is not null
           |    and time_stamp is not null
           |)
           |
           |, latest_per_user_item as (
           |  select
           |    user_id,
           |    item_id,
           |    rating,
           |    timestamp,
           |    row_number() over (partition by user_id, item_id order by cast(time_stamp as bigint) desc) as rn
           |  from latest_per_user_item_ranked
           |)
           |
           |, deduped as (
           |  select user_id, item_id, rating, timestamp
           |  from latest_per_user_item
           |  where rn = 1
           |)
           |
           |, topn_per_user_ranked as (
           |  select
           |    user_id,
           |    concat(item_id, ':', cast(rating as string), ':', cast(timestamp as string)) as feature,
           |    row_number() over (partition by user_id order by cast(timestamp as bigint) desc) as rk
           |  from deduped
           |)
           |
           |, topn_per_user as (
           |  select user_id, feature, rk
           |  from topn_per_user_ranked
           |  where rk <= $TOP_N
           |)
           |
           |, agg_result as (
           |  select
           |    user_id,
           |    sort_array(collect_list(named_struct('rk', rk, 'feature', feature))) as arr
           |  from topn_per_user
           |  group by user_id
           |)
           |
           |select
           |  user_id,
           |  concat_ws(',', transform(arr, x -> x.feature)) as feature
           |from agg_result
           |""".stripMargin

      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      DataQualityChecker.check(result, "user_app_rate_sequence", outputPath)
      result.show()
      result.printSchema()

      result
        .selectExpr("user_id", "feature")
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
