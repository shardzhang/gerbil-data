package processing.clean

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object AliCtrCleanSample {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: AliCtrCleanSample <path>")

    val path = args(0)
    val outputPath = s"$path/clean_sample"
    green_println(s"path = $path, outputPath: $outputPath")

    setLogLevel()

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      // user,time_stamp,adgroup_id,pid,nonclk,clk
      // 581738,1494137644,1,430548_1007,1,0
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"$path/raw_sample.csv")
        .createOrReplaceTempView("raw_sample")

      val sql =
        """
          | select
          |   cast(user as string) as user_id,
          |   cast(adgroup_id as string) as item_id,
          |   cast(clk as int) as clk,
          |   cast(nonclk as int) as nonclk,
          |   cast(time_stamp as bigint) as time_stamp,
          |   pid,
          |   from_unixtime(cast(time_stamp as bigint), 'yyyy-MM-dd') AS day
          | from raw_sample
          | where user is not null
          | and adgroup_id is not null
          | and clk is not null
          | and time_stamp is not null
          |""".stripMargin
      green_println(f"sql: ${sql}")

      val cleanSample = spark.sql(sql).cache()
      green_println(s"cleanSample.count() = ${cleanSample.count()}")

      DataQualityChecker.check(cleanSample, "clean_sample", outputPath)
      cleanSample.show()
      cleanSample.printSchema()

      cleanSample
        .selectExpr("user_id", "item_id", "clk", "nonclk", "time_stamp", "pid", "day")
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
 ~/Py/gerbil-data/b/proc/clean  on main  sh AliCtrCleanSample.sh                                                    ✔  took 7m 35s  at 18:06:12
 >>> Transformed | path = /Users/dazhang/PycharmProject/data/Ali_Display_Ad_Click, outputPath: /Users/dazhang/PycharmProject/data/Ali_Display_Ad_Click/clean_sample
 >>> Transformed | sql:
 select
 cast(user as string) as user_id,
 cast(adgroup_id as string) as item_id,
 cast(clk as int) as clk,
 cast(nonclk as int) as nonclk,
 cast(time_stamp as bigint) as time_stamp,
 pid,
 from_unixtime(cast(time_stamp as bigint), 'yyyy-MM-dd') AS day
 from raw_sample
 where user is not null
 and adgroup_id is not null
 and clk is not null
 and time_stamp is not null

 >>> Transformed | cleanSample.count() = 26557961

================================================================================
DATA QUALITY CHECK: clean_sample
================================================================================
  Total records:  26557961
 ─ user_id (string)
 NULL:       0 (0.00%)
 Cardinality: 1141729
 ─ item_id (string)
 NULL:       0 (0.00%)
 Cardinality: 846811
 ─ clk (int)
 NULL:       0 (0.00%)
 Min:        0.0
 Max:        1.0
 Mean:       0.0514
 Stddev:     0.2209
 ─ nonclk (int)
 NULL:       0 (0.00%)
 Min:        0.0
 Max:        1.0
 Mean:       0.9486
 Stddev:     0.2209
 ─ time_stamp (bigint)
 NULL:       0 (0.00%)
 Min:        1.494E9
 Max:        1.494691186E9
 Mean:       1494354798.1415
 Stddev:     198755.2618
 ─ pid (string)
 NULL:       0 (0.00%)
 Cardinality: 2
 ─ day (string)
 NULL:       0 (0.00%)
 Cardinality: 8
================================================================================
 >>> Transformed | [Drift] No previous stats at /Users/dazhang/PycharmProject/data/Ali_Display_Ad_Click/_quality/clean_sample_stats.json, skipping drift detection.
+-------+-------+---+------+----------+-----------+----------+
|user_id|item_id|clk|nonclk|time_stamp|        pid|       day|
+-------+-------+---+------+----------+-----------+----------+
| 581738|      1|  0|     1|1494137644|430548_1007|2017-05-07|
| 449818|      3|  0|     1|1494638778|430548_1007|2017-05-13|
| 914836|      4|  0|     1|1494650879|430548_1007|2017-05-13|
| 914836|      5|  0|     1|1494651029|430548_1007|2017-05-13|
| 399907|      8|  0|     1|1494302958|430548_1007|2017-05-09|
| 628137|      9|  0|     1|1494524935|430548_1007|2017-05-12|
| 298139|      9|  0|     1|1494462593|430539_1007|2017-05-11|
| 775475|      9|  0|     1|1494561036|430548_1007|2017-05-12|
| 555266|     11|  0|     1|1494307136|430539_1007|2017-05-09|
| 117840|     11|  0|     1|1494036743|430548_1007|2017-05-06|
| 739815|     11|  0|     1|1494115387|430539_1007|2017-05-07|
| 623911|     11|  0|     1|1494625301|430548_1007|2017-05-13|
| 623911|     11|  0|     1|1494451608|430548_1007|2017-05-11|
| 421590|     11|  0|     1|1494034144|430548_1007|2017-05-06|
| 976358|     13|  0|     1|1494156949|430548_1007|2017-05-07|
| 286630|     13|  0|     1|1494218579|430539_1007|2017-05-08|
| 286630|     13|  0|     1|1494289247|430539_1007|2017-05-09|
| 771431|     13|  0|     1|1494153867|430548_1007|2017-05-07|
| 707120|     13|  0|     1|1494220810|430548_1007|2017-05-08|
| 530454|     13|  0|     1|1494293746|430548_1007|2017-05-09|
+-------+-------+---+------+----------+-----------+----------+
only showing top 20 rows

root
 |-- user_id: string (nullable = true)
 |-- item_id: string (nullable = true)
 |-- clk: integer (nullable = true)
 |-- nonclk: integer (nullable = true)
 |-- time_stamp: long (nullable = true)
 |-- pid: string (nullable = true)
 |-- day: string (nullable = true)
 */