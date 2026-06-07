package feature

import org.apache.spark.sql.SparkSession
import utils.LogUtils.green_println
import utils.LogUtils.setLogLevel

/**
 * @author shard zhang
 * @date 2026/6/4 11:42
 * @note
 */
object ML1MMovieStatFeature {
  private val TOP_N = 200
  private val RAW_SEP = "::"
  private val SEP = "\t"

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

    val sql = s"""
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
           |   stat.item_id as movie_id,        --电影ID
           |   title as movie_title,            --电影标题
           |   genres as movie_genres,          --电影类型
           |   movie_genre_cnt, --电影类型数
           |   movie_rate_count,                --电影评分数
           |   movie_avg_rate,                  --电影平均评分
           |   row_number() over (order by movie_rate_count desc) as movie_hot_rank --电影热门排名
           | from stat inner join movie_t 
           | on stat.item_id = movie_t.item_id
    """.stripMargin

      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      result.show()
      result.printSchema()

      // 如果outputPath不存在，创建它
      spark.sparkContext
      .hadoopConfiguration()
      .getFileSystem(new Path(outputPath))
      .mkdirs(new Path(outputPath), true)

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
