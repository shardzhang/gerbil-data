package processing.clean

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object MobileRecCleanSample {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: MobileRecCleanSample <path>")
    val path = args(0)
    val outputPath = s"$path/clean_sample"
    green_println(s"path = $path, outputPath: $outputPath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      // app_package,review,rating,votes,date,uid,formated_date,unix_timestamp,app_category
      // com.cleverapps.heroes,It's really a fun game,5,1,"October 21, 2018",shqoc6X1fcJRLEmx,2018-10-21,1540094400.0,Casual
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"$path/interactions/mobilerec_final.csv")
        .createOrReplaceTempView("interactions_raw")

      val sql =
        s"""
           | select
           |    cast(uid as string) as user_id,
           |    cast(app_package as string) as item_id,
           |    cast(rating as int) as rating,
           |    cast(votes as int) as votes,
           |    cast(unix_timestamp as bigint) as time_stamp,
           |    formated_date as day,
           |    app_category as category,
           |    cast(review as string) as review
           | from interactions_raw
           | where uid is not null and length(uid) > 0
           | and app_package is not null and length(app_package) > 0
           | and rating is not null and rating >= 1 and rating <= 5
           | and unix_timestamp is not null
           |""".stripMargin
      green_println(f"sql: ${sql}")

      val cleanSample = spark.sql(sql).cache()
      green_println(s"cleanSample.count() = ${cleanSample.count()}")

      DataQualityChecker.check(cleanSample, "clean_sample", outputPath)
      cleanSample.show()
      cleanSample.printSchema()

      cleanSample
        .selectExpr("user_id", "item_id", "rating", "votes", "time_stamp", "day", "category", "review")
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

/**
 ~/Py/gerbil-data/b/proc/clean  on main +4 !1  sh MobileRecCleanSample.sh                                                    101 ✘  at 17:57:12
 >>> Transformed | path = /Users/dazhang/PycharmProject/data/mobile_rec_data, outputPath: /Users/dazhang/PycharmProject/data/mobile_rec_data/clean_sample
 >>> Transformed | cleanSample.count() = 19064022
================================================================================
DATA QUALITY CHECK: clean_sample
================================================================================
  Total records:  19064022
 ─ user_id (string)
 NULL:       0 (0.00%)
 Cardinality: 700166
 ─ item_id (string)
 NULL:       0 (0.00%)
 Cardinality: 10174
 ─ rating (int)
 NULL:       0 (0.00%)
 Min:        1.0
 Max:        5.0
 Mean:       3.2899
 Stddev:     1.5333
 ─ votes (int)
 NULL:       57 (0.00%)
 Min:        0.0
 Max:        193537.0
 Mean:       10.2644
 Stddev:     129.6796
 ─ time_stamp (bigint)
 NULL:       60 (0.00%)
 Min:        0.0
 Max:        1.6499088E9
 Mean:       1600373928.6270
 Stddev:     51635618.6536
 ─ day (string)
 NULL:       0 (0.00%)
 Cardinality: 3491
 ─ category (string)
 NULL:       1 (0.00%)
 Cardinality: 122
 ─ review (string)
 NULL:       551 (0.00%)
 Cardinality: 15471187
================================================================================
 >>> Transformed | [Drift] No previous stats at /Users/dazhang/PycharmProject/data/mobile_rec_data/_quality/clean_sample_stats.json, skipping drift detection.
+----------------+--------------------+------+-----+----------+----------+-----------------+--------------------+
|         user_id|             item_id|rating|votes|time_stamp|       day|         category|              review|
+----------------+--------------------+------+-----+----------+----------+-----------------+--------------------+
|shqoc6X1fcJRLEmx|com.cleverapps.he...|     5|    1|1540094400|2018-10-21|           Casual|It's really a fun...|
|shqoc6X1fcJRLEmx|        com.bodyfast|     2|    0|1547787600|2019-01-18| Health & Fitness|uninstalling. it ...|
|shqoc6X1fcJRLEmx|com.thrivegames.w...|     4|    1|1610773200|2021-01-16|             Word|      Love this game|
|shqoc6X1fcJRLEmx|com.affinity.rewa...|     1|    1|1635998400|2021-11-04|    Entertainment|Doesn't update pl...|
|shqoc6X1fcJRLEmx|dating.inmessage.net|     1|    0|1637730000|2021-11-24|           Dating|app crashes every...|
|smlNgCSD1z66dtpP|com.kingsoopers.m...|     4|    1|1426824000|2015-03-20|         Shopping|We are having tec...|
|smlNgCSD1z66dtpP|org.inaturalist.a...|     4|    0|1427515200|2015-03-28|        Education|           Good one.|
|smlNgCSD1z66dtpP|          via.driver|     5|    0|1494302400|2017-05-09|         Business|   The best service.|
|smlNgCSD1z66dtpP|com.outfit7.mytal...|     5|  718|1564804800|2019-08-03|           Casual|I really love you...|
|smlNgCSD1z66dtpP|com.kasindev.skin...|     5|    2|1620446400|2021-05-08| Libraries & Demo|free fire is time...|
|smlNgCSD1z66dtpP|com.myfitnesspal....|     2|    0|1641704400|2022-01-09| Health & Fitness|No security wat h...|
|smnwlsh9CjgHk8Ul|com.wunderground....|     1|    9|1579323600|2020-01-18|          Weather|Used to be the be...|
|smnwlsh9CjgHk8Ul|com.cookapps.wond...|     1|   19|1603339200|2020-10-22|           Puzzle|The character des...|
|smnwlsh9CjgHk8Ul| com.unvoid.borealis|     5|    0|1610254800|2021-01-10|  Personalization|The best icon pac...|
|smnwlsh9CjgHk8Ul|   com.beachbody.bod|     4|    9|1632542400|2021-09-25| Health & Fitness|Many different op...|
|smnwlsh9CjgHk8Ul|com.playhardlab.h...|     4|    0|1648872000|2022-04-02|       Simulation|Very good but the...|
|snCCzP0FvzSb0p8A|com.roadwarrior.a...|     4|    0|1520139600|2018-03-04|Maps & Navigation|The app for the m...|
|snCCzP0FvzSb0p8A|com.jumpgames.Rea...|     1|    0|1570680000|2019-10-10|           Action|Bad game ever I w...|
|snCCzP0FvzSb0p8A|com.szckhd.jwgly....|     4|    2|1595995200|2020-07-29|     Role Playing|   Problem resolved.|
|snCCzP0FvzSb0p8A|com.netgear.netge...|     1|    2|1642914000|2022-01-23|     Productivity|Can't get the pro...|
+----------------+--------------------+------+-----+----------+----------+-----------------+--------------------+
only showing top 20 rows

root
 |-- user_id: string (nullable = true)
 |-- item_id: string (nullable = true)
 |-- rating: integer (nullable = true)
 |-- votes: integer (nullable = true)
 |-- time_stamp: long (nullable = true)
 |-- day: string (nullable = true)
 |-- category: string (nullable = true)
 |-- review: string (nullable = true)
 */