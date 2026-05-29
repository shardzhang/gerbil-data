package sample

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, size, split}

import scala.util.control.NonFatal

/**
 * @author shard zhang
 * @date 2026/5/29 13:59
 * @note
 */
object TestHive {

    private val DatabaseName = "ml_1m_db"
    private val RatingsTable = s"$DatabaseName.ratings"
    private val RatingsFile = "ratings.dat"
    private val DefaultWarehouseDir = "file:///Users/dazhang/PycharmProject/gerbil-data/bash/state/hive12/warehouse"

    private def setLogLevel(): Unit = {
        Logger.getRootLogger.setLevel(Level.WARN)
    }

    private def ensureRatingsTable(spark: SparkSession): Unit = {
        val ml1mPath = sys.env.getOrElse(
            "ML_1M_PATH",
            throw new IllegalArgumentException("ML_1M_PATH is not set")
        )
        val ratingsPath = new Path(s"$ml1mPath/$RatingsFile")
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val warehouseDir = spark.conf.getOption("spark.sql.warehouse.dir")
            .orElse(Option(spark.sparkContext.hadoopConfiguration.get("hive.metastore.warehouse.dir")))
            .getOrElse(DefaultWarehouseDir)
        val warehousePath = new Path(warehouseDir)
        val tablePath = new Path(warehousePath, s"$DatabaseName.db/ratings")

        require(fs.exists(ratingsPath), s"ratings file not found: ${ratingsPath.toString}")

        spark.sql(s"CREATE DATABASE IF NOT EXISTS $DatabaseName")

        val tableReady = try {
            val hasTable = spark.sql(s"SHOW TABLES IN $DatabaseName LIKE 'ratings'").head(1).nonEmpty
            hasTable &&
                spark.table(RatingsTable).columns.sameElements(Array("user_id", "movie_id", "rating", "ts")) &&
                spark.table(RatingsTable).limit(1).count() > 0
        } catch {
            case NonFatal(_) => false
        }

        if (!tableReady) {
            spark.sql(s"DROP TABLE IF EXISTS $RatingsTable")
            if (fs.exists(tablePath)) {
                fs.delete(tablePath, true)
            }

            spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

            val ratingsDF = spark.read.text(ratingsPath.toString)
                .withColumn("parts", split(col("value"), "::"))
                .filter(size(col("parts")) === 4)
                .select(
                    col("parts")(0).cast("int").as("user_id"),
                    col("parts")(1).cast("int").as("movie_id"),
                    col("parts")(2).cast("int").as("rating"),
                    col("parts")(3).cast("long").as("ts")
                )

            ratingsDF.write
                .format("parquet")
                .mode("overwrite")
                .saveAsTable(RatingsTable)
        }
    }

    // 帮我写代码, 运行select * from ml_1m_db.ratings limit 10;
    def main(args: Array[String]): Unit = {
        setLogLevel()

        val spark = org.apache.spark.sql.SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .enableHiveSupport()  // 关联本地 Hive 元数据
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        ensureRatingsTable(spark)

        val df = spark.sql("select * from ml_1m_db.ratings limit 10")
        df.show()

        spark.stop()
    }

}
