package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import utils.LogUtils.{green_println, setLogLevel}

/**
 * @author shard zhang
 * @date 2026/5/28 17:45
 * @note ML-1M 处理 ml-1m 原始评分数据, 清洗并输出 CSV
 *
*       DataFrame 负责读原始文本并注册临时表，SQL 负责全部核心业务逻辑
  *
*       输入:
  *       ratings.dat
*       格式: UserID::MovieID::Rating::Timestamp
*
*       输出:
  *       user_id \t item_id \t rating \t time_stamp \t day
    *
*       逻辑:
  *       1. 读取原始文本并拆分字段
  *       2. 过滤非法数据
  */
object ML1MCleanSample {
  private val RAW_SEP = "::"
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: ML1MCleanSample <path>")
    val path = args(0)
    val outputPath = s"$path/clean_sample"
    green_println(s"path = $path, outputPath: ${outputPath}")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN") // org.apache.spark
    green_println("Spark Version: " + spark.version)

    try {
      // user_id::movie_id::rating::timestamp
      spark.read.text(s"$path/ratings.dat").createOrReplaceTempView("ratings_raw")

      val sql =
        s"""
           | with clean as (
           |   select
           |     user_id,
           |     item_id,
           |     rating,
           |     time_stamp,
           |     day,
           |     row_number() over (partition by user_id, item_id, rating, time_stamp order by time_stamp desc) as rn
           |   from (
           |     SELECT
           |       trim(split(value, '${RAW_SEP}')[0]) AS user_id,
           |       trim(split(value, '${RAW_SEP}')[1]) AS item_id,
           |       CAST(trim(split(value, '${RAW_SEP}')[2]) AS INT) AS rating,
           |       CAST(trim(split(value, '${RAW_SEP}')[3]) AS LONG) AS time_stamp,
           |       from_unixtime(CAST(split(value, '${RAW_SEP}')[3] AS LONG), 'yyyy-MM-dd') AS day
           |     FROM ratings_raw
           |     WHERE size(split(value, '${RAW_SEP}')) = 4
           |   ) t
           |   where user_id is not null and user_id >= 0
           |   and item_id is not null and item_id >= 0
           |   and rating is not null
           |   and time_stamp is not null
           |   and rating >= 1 and rating <= 5
           | )
           |
           | select
           |   user_id,
           |   item_id,
           |   rating,
           |   time_stamp,
           |   day
           | from clean
           | where rn = 1
           |""".stripMargin
      val cleanSample = spark.sql(sql).cache()
      green_println(s"cleanSample.count() = ${cleanSample.count()}")
      cleanSample.show()
      cleanSample.printSchema()

      // 4. 输出
      cleanSample
        .selectExpr("user_id", "item_id", "rating", "time_stamp", "day")
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
