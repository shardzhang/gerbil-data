package sample

import org.apache.spark.sql.SparkSession
import utils.LogUtils.{green_println, setLogLevel}

/**
 * @author shard zhang
 * @date 2026/5/29 12:09
 * @note ML-1M 读取原始样本, 拼接用户特征, 用户行为序列特征, 形成最终明文样本
 *
 *       DataFrame 负责读原始文本并注册临时表，SQL 负责全部核心业务逻辑
 *
 *       输入:
 *       users.dat
 *       movies.dat
 *       clean_sample.csv
 *       user_movie_rate.csv
 *
 *       输出:
 *       user_id \t item_id \t time_stamp \t rating \t day \t user_profile \t item_feature \t user_behavior
 *
 *       逻辑:
 *       1. 读取原始文本并拆分字段
 *       2. 过滤非法数据
 *       3. 拼接用户特征 + 物品特征 + 用户行为序列特征
 *       4. 形成最终明文样本
 */
object ML1MJoinSample {
  private val RAW_SEP = "::"
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: ML1MJoinSample <path>")
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
           |     s.user_id,
           |     s.item_id,
           |     s.time_stamp,
           |     s.rating,
           |     s.day,
           |     COALESCE(u.feature, '{}') AS user_profile,
           |     COALESCE(i.feature, '{}') AS item_feature,
           |     to_json(
           |        named_struct(
           |          'user_movie_rate', COALESCE(r.feature, '')
           |        )
           |     ) AS user_behavior
           | FROM clean_sample s
           | LEFT JOIN user_profile u
           | ON s.user_id = u.user_id
           | LEFT JOIN item_feature i
           | ON s.item_id = i.item_id
           | LEFT JOIN user_movie_rate r
           | ON s.user_id = r.user_id
           |""".stripMargin
      val joinSample = spark.sql(sql).cache()
      green_println(f"joinSample.count(): ${joinSample.count()}")
      joinSample.show()
      joinSample.printSchema()

      joinSample
        .selectExpr("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "item_feature", "user_behavior")
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
