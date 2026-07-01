package processing.alictr

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object AliCtrUserBehaviorSequence {
  private val TOP_N = 200
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: AliCtrUserBehaviorSequence <path>")
    val path = args(0)
    val outputPath = s"$path/user_ad_sequence"
    green_println(s"path = $path, outputPath: $outputPath")
    setLogLevel()
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      spark.read.option("sep", SEP).csv(s"$path/clean_sample")
        .toDF("user_id", "item_id", "clk", "nonclk", "time_stamp", "pid")
        .createOrReplaceTempView("clean_sample")
      val sql = s"""
        |with deduped as (
        |  select user_id, item_id, clk, cast(time_stamp as bigint) as timestamp,
        |    row_number() over (partition by user_id, item_id, clk, time_stamp order by cast(time_stamp as bigint) desc) as rn
        |  from clean_sample
        |  where user_id is not null and length(user_id) > 0 and item_id is not null and length(item_id) > 0 and clk is not null)
        |, latest_only as (select user_id, item_id, clk, timestamp from deduped where rn = 1)
        |, topn_ranked as (select user_id, concat(item_id, ':', clk) as feature,
        |    row_number() over (partition by user_id order by timestamp desc) as rk from latest_only)
        |, topn as (select user_id, feature, rk from topn_ranked where rk <= $TOP_N)
        |, agg as (select user_id, sort_array(collect_list(named_struct('rk', rk, 'feature', feature))) as arr from topn group by user_id)
        |select user_id, concat_ws(',', transform(arr, x -> x.feature)) as feature from agg""".stripMargin
      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      DataQualityChecker.check(result, "user_ad_sequence", outputPath)
      result.selectExpr("user_id", "feature").write.mode("overwrite").option("sep", SEP).csv(outputPath)
      result.unpersist()
    } finally { spark.stop() }
  }
}
