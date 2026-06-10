package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import utils.LogUtils.{green_println, setLogLevel}

/**
 * @author shard zhang
 * @date 2026/5/28 17:45
 * @note ML-1M processes raw ml-1m rating data, cleans and outputs CSV
 *       Cleans raw ML-1M ratings into deduplicated CSV with day field.
 *
 *       DataFrames load raw text; SQL handles all transformation logic.
 *       DataFrames load raw text; SQL handles all transformation logic.
 *
 *       Input:
 *       ratings.dat
 *       Format: UserID::MovieID::Rating::Timestamp
 *
 *       Output:
 *       user_id \t item_id \t rating \t time_stamp \t day
 *
 *       Logic:
 *       1. Parse raw text and split fields
 *          2. Filter invalid data
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

      // Deduplicate: for identical (user, item, rating, timestamp), keep only the latest record
      val sql =
        s"""
           | with clean as (
           |   select
           |     user_id,
           |     item_id,
           |     rating,
           |     time_stamp,
           |     day,
           |     row_number() over (partition by user_id, item_id, rating, time_stamp order by time_stamp desc) as rn  -- dedup identical rows
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
           |   where user_id is not null and cast(user_id as bigint) >= 0
           |   and item_id is not null and cast(item_id as bigint) >= 0
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

      // 4. Output
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
