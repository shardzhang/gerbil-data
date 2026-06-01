package sample

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import sample.CleanSampleV1.SEP
import utils.LogUtils.{green_println, setLogLevel}

/**
 * @author shard zhang
 * @date 2026/5/29 12:09
 * @note 读取原始样本, 拼接用户特征, 用户行为序列特征, 形成最终明文样本
 */
object JoinSampleV1 {
  private val RAW_SEP = "::"
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      green_println(s"Usage: ${this.getClass.getSimpleName.stripSuffix("$")} <data_path>")
      System.exit(1)
    }
    val path = args(0)
    val savePath = s"$path/join_sample"
    green_println(s"path = $path, save_path = $savePath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println("Spark Version: " + spark.version)

    try {
      // users.txt
      spark.read.text(s"$path/users.dat").createOrReplaceTempView("users")
      // movies.txt
      spark.read.text(s"$path/movies.dat").createOrReplaceTempView("movies")
      // clean_sample.csv
      spark.read
        .option("sep", SEP)
        .csv(s"$path/clean_sample")
        .toDF("user_id", "item_id", "rating", "time_stamp", "day")
        .createOrReplaceTempView("clean_sample")
      // user_movie_rate.csv
      spark.read
        .option("sep", SEP)
        .csv(s"$path/user_movie_rate")
        .toDF("user_id", "feature")
        .createOrReplaceTempView("user_movie_rate")

      val sql =
        s"""
           |WITH user_profile AS (
           |  select user_id, feature
           |  from
           |  (
           |    SELECT
           |        user_id,
           |        to_json(struct(user_id, gender, age, occupation, zip_code)) AS feature,
           |        row_number() OVER (PARTITION BY user_id, gender, age, occupation, zip_code ORDER BY user_id DESC) AS rn
           |    FROM (
           |        SELECT
           |            split(value, '$RAW_SEP')[0] AS user_id,
           |            split(value, '$RAW_SEP')[1] AS gender,
           |            split(value, '$RAW_SEP')[2] AS age,
           |            split(value, '$RAW_SEP')[3] AS occupation,
           |            split(value, '$RAW_SEP')[4] AS zip_code
           |        FROM users
           |        WHERE size(split(value, '$RAW_SEP')) = 5
           |    ) t
           |    WHERE user_id IS NOT NULL AND user_id != ''
           |  ) clean
           |  where rn = 1
           |)
           |
           |, item_feature AS (
           |  select item_id, feature
           |  from
           |  (
           |    SELECT
           |        item_id,
           |        to_json(struct(item_id, title, genres)) AS feature,
           |        row_number() OVER (PARTITION BY item_id, title, genres ORDER BY item_id DESC) AS rn
           |    FROM (
           |        SELECT
           |            split(value, '$RAW_SEP')[0] AS item_id,
           |            split(value, '$RAW_SEP')[1] AS title,
           |            split(split(value, '$RAW_SEP')[2], '\\\\|') AS genres
           |        FROM movies
           |        WHERE size(split(value, '$RAW_SEP')) = 3
           |    ) t
           |    WHERE item_id IS NOT NULL AND item_id != ''
           |  ) clean
           |  where rn = 1
           |)
           |
           | SELECT
           |     r.user_id,
           |     r.item_id,
           |     r.time_stamp,
           |     r.rating,
           |     r.day,
           |     COALESCE(u.feature, '{}') AS user_profile,
           |     COALESCE(i.feature, '{}') AS item_feature,
           |     to_json(named_struct('user_movie_rate', COALESCE(b.feature, ''))) AS user_behavior
           | FROM clean_sample r
           | LEFT JOIN user_profile u
           | ON r.user_id = u.user_id
           | LEFT JOIN item_feature i
           | ON r.item_id = i.item_id
           | LEFT JOIN user_movie_rate b
           | ON r.user_id = b.user_id
           |""".stripMargin
      val result = spark.sql(sql).cache()
      green_println(f"result.count(): ${result.count()}")

      result
        .selectExpr("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "item_feature", "user_behavior")
        .write
        .mode("overwrite")
        .option("sep", SEP)
        .csv(savePath)
      result.unpersist()
    } finally {
      spark.stop()
    }
  }
}
