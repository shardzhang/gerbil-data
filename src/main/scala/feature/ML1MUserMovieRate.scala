package feature

import org.apache.spark.sql.SparkSession
import utils.LogUtils.{green_println, setLogLevel}

/**
 * @author shard zhang
 * @date 2026/5/29 18:18
 * @note ML-1M 用户电影评分序列构造
 *
 *       DataFrame 负责读原始文本并注册临时表，SQL 负责全部核心业务逻辑
 *
 *       输入:
 *       ratings.dat
 *       格式: UserID::MovieID::Rating::Timestamp
 *
 *       输出:
 *       user_id \t item_id:rating:timestamp,item_id:rating:timestamp,...
 *
 *       逻辑:
 *       1. 读取原始文本并拆分字段
 *       2. 过滤非法数据
 *       3. 对同一 (user_id, item_id) 保留最新一次评分
 *       4. 对每个 user_id 保留最近 200 个 item
 *       5. 聚合成行为序列
 */
object ML1MUserMovieRate {
  private val TOP_N = 200
  private val RAW_SEP = "::"
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: ML1MUserMovieRate <input_base_path>")
    val basePath = args(0)
    val inputPath = s"$basePath/ratings.dat"
    val outputPath = s"$basePath/user_movie_rate"
    green_println(f"basePath: ${basePath}, inputPath: ${inputPath}, outputPath: ${outputPath}")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    green_println("Spark Version: " + spark.version)

    try {
      // UserID::MovieID::Rating::Timestamp
      spark.read.text(inputPath).createOrReplaceTempView("ratings")

      val sql =
        s"""
           | WITH parsed AS (
           |   SELECT
           |       trim(split(value, '$RAW_SEP')[0]) AS user_id,
           |       trim(split(value, '$RAW_SEP')[1]) AS item_id,
           |       CAST(split(value, '$RAW_SEP')[2] AS INT) AS rating,
           |       CAST(split(value, '$RAW_SEP')[3] AS BIGINT) AS timestamp
           |   FROM ratings
           |   WHERE size(split(value, '$RAW_SEP')) = 4
           | )
           | 
           | , latest_per_user_item_ranked AS (
           |   SELECT
           |     user_id,
           |     item_id,
           |     rating,
           |     timestamp,
           |     row_number() OVER (PARTITION BY user_id, item_id ORDER BY timestamp DESC) AS rn
           |   FROM parsed
           |   WHERE user_id IS NOT NULL AND length(user_id) > 0
           |     AND item_id IS NOT NULL AND length(item_id) > 0
           |     AND rating IS NOT NULL
           |     AND timestamp IS NOT NULL
           | )
           | 
           | , latest_per_user_item AS (
           |   SELECT
           |     user_id,
           |     item_id,
           |     rating,
           |     timestamp
           |   FROM latest_per_user_item_ranked
           |   WHERE rn = 1
           | )
           | 
           | , topn_per_user_ranked AS (
           |   SELECT
           |     user_id,
           |     concat(
           |       item_id, ':',
           |       cast(rating as string), ':',
           |       cast(timestamp as string)
           |     ) AS feature,
           |     row_number() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS rk
           |   FROM latest_per_user_item
           | )
           | 
           | , topn_per_user AS (
           |   SELECT
           |     user_id,
           |     feature,
           |     rk
           |   FROM topn_per_user_ranked
           |   WHERE rk <= $TOP_N
           | )
           | 
           | , agg_result AS (
           |   SELECT
           |     user_id,
           |     sort_array(collect_list(named_struct('rk', rk, 'feature', feature))) AS arr
           |   FROM topn_per_user
           |   GROUP BY user_id
           | )
           | 
           | SELECT
           |   user_id,
           |   concat_ws(',', transform(arr, x -> x.feature)) AS feature
           | FROM agg_result
           |""".stripMargin

      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
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
