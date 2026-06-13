package processing.feature

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.green_println
import utils.LogUtils.setLogLevel

/**
 * Computes movie statistical features: average rating, rating count, genre count, and hot rank.
 */
object ML1MMovieStatFeature {
  /** Raw data separator in ML-1M movies.dat (double colon). */
  private val RAW_SEP = "::"
  /** Output field separator (tab). */
  private val SEP = "\t"

  /** CLI entry point: computes per-movie statistics (avg rating, count, genre count, hot rank). */
  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: ML1MUserMovieRate <input_base_path>")
    val basePath = args(0)
    val inputPath = s"$basePath/ratings.dat"
    val outputPath = s"$basePath/item_feature"
    green_println(f"basePath: ${basePath}, inputPath: ${inputPath}, outputPath: ${outputPath}")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    green_println("Spark Version: " + spark.version)

    try {
      // movies.dat
      spark.read.text(s"$basePath/movies.dat").createOrReplaceTempView("movies")
      // clean_sample.csv
      spark.read
        .option("sep", SEP)
        .csv(s"$basePath/clean_sample")
        .toDF("user_id", "item_id", "rating", "time_stamp", "day")
        .createOrReplaceTempView("clean_sample")

      // movie_t: parse movie metadata; stat: compute per-movie rating stats; then join and rank by popularity
      val sql =
        s"""
           | with movie_t as (
           |   SELECT
           |     split(value, '$RAW_SEP')[0] AS item_id,
           |     split(value, '$RAW_SEP')[1] AS title,
           |     split(value, '$RAW_SEP')[2] AS genres,
           |     size(split(split(value, '$RAW_SEP')[2], '\\\\|')) as movie_genre_cnt
           | FROM movies
           | WHERE size(split(value, '$RAW_SEP')) = 3
           |)
           | 
           | , stat as (
           | select 
           |  item_id, 
           |  ROUND(AVG(CAST(rating AS DOUBLE)), 2) AS movie_avg_rate,
           |  count(distinct user_id) as movie_rate_count
           | from clean_sample
           | group by item_id
           |)
           |
           | select
           |   stat.item_id as movie_id,        --movie ID
           |   title as movie_title,            --movie title
           |   genres as movie_genres,          --movie genres
           |   movie_genre_cnt, --movie genre count
           |   movie_rate_count,                --movie rating count
           |   movie_avg_rate,                  --movie average rating
           |   row_number() over (order by movie_rate_count desc) as movie_hot_rank --movie hot rank
           | from stat inner join movie_t 
           | on stat.item_id = movie_t.item_id
    """.stripMargin

      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      DataQualityChecker.check(result, "item_feature", outputPath)
      result.show()
      result.printSchema()

      result
        .select("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
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
DATA QUALITY CHECK: item_feature
================================================================================
  Total records:  3706
   ─ movie_id (string)
     NULL:       0 (0.00%)
     Cardinality: 3706
   ─ movie_title (string)
     NULL:       0 (0.00%)
     Cardinality: 3706
   ─ movie_genres (string)
     NULL:       0 (0.00%)
     Cardinality: 301
   ─ movie_genre_cnt (int)
     NULL:       0 (0.00%)
     Min:        1.0
     Max:        6.0
     Mean:       1.6708
     Stddev:     0.8113
   ─ movie_rate_count (bigint)
     NULL:       0 (0.00%)
     Min:        1.0
     Max:        3428.0
     Mean:       269.8891
     Stddev:     384.0478
   ─ movie_avg_rate (double)
     NULL:       0 (0.00%)
     Min:        1.0
     Max:        5.0
     Mean:       3.2390
     Stddev:     0.6729
   ─ movie_hot_rank (int)
     NULL:       0 (0.00%)
     Min:        1.0
     Max:        3706.0
     Mean:       1853.5000
     Stddev:     1069.9744
================================================================================
 */