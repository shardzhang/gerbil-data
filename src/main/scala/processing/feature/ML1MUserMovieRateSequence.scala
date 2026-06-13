package processing.feature

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

/**
 *       Builds per-user movie rating sequences, keeping top-N most recent per user.
 * ML-1M user movie rating sequence construction
 *       DataFrames load raw text; SQL handles all transformation logic.
 *       DataFrames load raw text; SQL handles all transformation logic.
 *
 *       Input:
 *       ratings.dat
 *       Format: UserID::MovieID::Rating::Timestamp
 *
 *       Output:
 *       user_id \t item_id:rating:timestamp,item_id:rating:timestamp,...
 *
 *       Logic:
 *       1. Parse raw text and split fields
 *       2. Filter invalid data
 *       3. Keep latest rating per (user_id, item_id)
 *       4. Keep most recent 200 items per user_id
 *       5. Aggregate into behavior sequence
 */
object ML1MUserMovieRateSequence {
  /** Maximum number of most recent ratings kept per user. */
  private val TOP_N = 200
  /** Raw data separator in ML-1M ratings.dat (double colon). */
  private val RAW_SEP = "::"
  /** Output field separator (tab). */
  private val SEP = "\t"

  /** CLI entry point: builds per-user movie rating sequences (top-N most recent). */
  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: ML1MUserMovieRate <input_base_path>")
    val basePath = args(0)
    val inputPath = s"$basePath/ratings.dat"
    val outputPath = s"$basePath/user_movie_rate_sequence"
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

      // Steps: 1) parse raw ratings, 2) keep latest per (user, movie), 3) keep top 200 per user, 4) aggregate into sequence
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
           |   SELECT  -- format as "item_id:rating:timestamp" and rank by recency
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
           |   SELECT  -- keep only top 200 most recent ratings per user
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
      DataQualityChecker.check(result, "user_movie_rate_sequence", outputPath)
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

/**
================================================================================
DATA QUALITY CHECK: user_movie_rate_sequence
================================================================================
  Total records:  6040
   ─ user_id (string)
     NULL:       0 (0.00%)
     Cardinality: 6040
   ─ feature (string)
     NULL:       0 (0.00%)
     Cardinality: 6040
================================================================================
 */