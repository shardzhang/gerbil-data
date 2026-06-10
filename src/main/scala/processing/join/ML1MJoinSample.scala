package processing.join

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

/**
 * @author shard zhang
 * @date 2026/5/29 12:09
 * @note ML-1M reads raw samples, joins user profiles, item features, and user behavior sequences to form final plaintext samples
 *       Reads clean samples and joins user profiles, item features, and user behavior sequences.
 *
 *       DataFrames load raw text; SQL handles all transformation logic.
 *       DataFrames load raw text; SQL handles all transformation logic.
 *
 *       Input:
 *       users.dat
 *       movie_feature.csv
 *       clean_sample.csv
 *       user_movie_rate.csv
 *
 *       Output:
 *       user_id \t item_id \t time_stamp \t rating \t day \t user_profile \t item_feature \t user_behavior
 *
 *       Logic:
 *       1. Parse raw text and split fields
 *       2. Filter invalid data
 *       3. Join user profiles + item features + user behavior sequences
 *       4. Form final plaintext samples
 */
object ML1MJoinSample {
  /** Raw data separator in ML-1M users.dat/movies.dat. */
  private val RAW_SEP = "::"
  /** Output field separator (tab). */
  private val SEP = "\t"

  /** CLI entry point: joins clean samples with user profiles, movie features, and user behavior sequences. */
  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: ML1MJoinSample <path>")
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
      // users.dat
      spark.read.text(s"$path/users.dat").createOrReplaceTempView("users")
      // movies.dat
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
      // item_feature.csv
      spark.read
        .option("sep", SEP)
        .csv(s"$path/item_feature")
        .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
        .createOrReplaceTempView("movie_feature")
      
      // Assemble final sample: join clean sample with user profile, movie feature, and user behavior
      val sql =
        s"""
           |WITH user_profile AS (
           | SELECT
           |     user_id,
           |     to_json(struct(user_id, gender, age, occupation, zip_code)) AS feature
           | FROM
           | (
           |  SELECT
           |      split(value, '$RAW_SEP')[0] AS user_id,
           |      split(value, '$RAW_SEP')[1] AS gender,
           |      split(value, '$RAW_SEP')[2] AS age,
           |      split(value, '$RAW_SEP')[3] AS occupation,
           |      split(value, '$RAW_SEP')[4] AS zip_code
           |  FROM users
           | WHERE size(split(value, '$RAW_SEP')) = 5
           | ) t
           | WHERE user_id IS NOT NULL AND user_id != ''
           |)
           |
           |, item_feature as (
           | select 
           |  movie_id, 
           |  to_json(struct(movie_title, movie_genres, movie_genre_cnt, movie_rate_count, movie_avg_rate, movie_hot_rank)) as feature
           | from movie_feature
           |)
           |
           | SELECT
           |     s.user_id,
           |     s.item_id,
           |     s.time_stamp,
           |     s.rating,
           |     s.day,
           |     COALESCE(u.feature, '{}') AS user_profile,
           |     COALESCE(f.feature, '{}') AS movie_feature,
           |     to_json(
           |        named_struct(
           |          'user_movie_rate', COALESCE(r.feature, '')
           |        )
           |     ) AS user_behavior
           | FROM clean_sample s
           | LEFT JOIN user_profile u
           | ON s.user_id = u.user_id
           | LEFT JOIN user_movie_rate r
           | ON s.user_id = r.user_id
           | LEFT JOIN item_feature f
           | ON s.item_id = f.movie_id
           |""".stripMargin
      val joinSample = spark.sql(sql).cache()
      green_println(f"joinSample.count(): ${joinSample.count()}")
      DataQualityChecker.check(joinSample, "join_sample", savePath)
      joinSample.show()
      joinSample.printSchema()

      joinSample
        .selectExpr("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "movie_feature", "user_behavior")
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
