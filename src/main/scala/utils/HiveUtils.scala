package utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.LogUtils.green_println

/**
 * @author Shard Zhang
 * @date 2022/4/20 16:47
 * @note
 */
/** Hive table I/O utilities: save DataFrames to partitioned tables and print samples. */
object HiveUtils {
  /** Saves `data` to a Hive table and prints a sample of `logNumber` rows. */
  def saveAndPrint(data: DataFrame,
                   table: String,
                   day: String,
                   logNumber: Int,
                   spark: SparkSession,
                   part: Int = 10): Unit = {
    saveToHive(data, table, day, spark, part)
    printHiveInfo(table, day, logNumber, spark)
  }

  /** Saves `data` with a custom partition key/value and prints a sample. */
  def saveAndPrintWithPartitionName(data: DataFrame,
                                    table: String,
                                    day: String,
                                    partitionName: String,
                                    partitionValue: String,
                                    logNumber: Int,
                                    spark: SparkSession,
                                    part: Int = 10): Unit = {
    saveToHiveWithPartitionName(data, table, day, partitionName, partitionValue, spark, part)
    printHiveInfoWithPartitionName(table, day, partitionName, partitionValue, logNumber, spark)
  }

  /** Overwrites the Hive table partition `day` with the contents of `data`. */
  def saveToHive(data: DataFrame,
                 table: String,
                 day: String,
                 spark: SparkSession,
                 part: Int = 10): Unit = {

    data.repartition(part)
      .createOrReplaceTempView("tmp")

    val query =
      s"""
         | INSERT OVERWRITE TABLE $table partition (day = '%s')
         | SELECT * FROM tmp
         | """.stripMargin.format(day)
    green_println(s"sql: ${query}")

    spark.sql(query)
  }

  /** Prints a sample of `logNumber` rows from the given Hive table partition. */
  def printHiveInfo(table: String,
                    day: String,
                    logNumber: Int,
                    spark: SparkSession): Unit = {

    green_println(s"save_table = ${table}")
    val sql =
      s"""
         | select *
         | from ${table}
         | where day = '${day}'
         | limit ${logNumber}
         | """.stripMargin
    green_println(s"sql = ${sql}")

    for (item: Row <- spark.sql(sql).rdd.collect()) {
      green_println(item.toString())
    }
  }


  /** Overwrites a Hive partition with both `day` and a custom partition key/value. */
  def saveToHiveWithPartitionName(data: DataFrame,
                                  table: String,
                                  day: String,
                                  partitionName: String,
                                  partitionValue: String,
                                  spark: SparkSession,
                                  part: Int = 10): Unit = {

    data.repartition(part)
      .createOrReplaceTempView("tmp")

    val query =
      s"""
         | INSERT OVERWRITE TABLE $table partition (day = '${day}', ${partitionName} = '${partitionValue}')
         | SELECT * FROM tmp
         | """.stripMargin
    green_println(s"sql: ${query}")

    spark.sql(query)
  }

  /** Prints a sample from a table filtered by both `day` and a custom partition key. */
  def printHiveInfoWithPartitionName(table: String,
                                     day: String,
                                     partitionName: String,
                                     partitionValue: String,
                                     logNumber: Int,
                                     spark: SparkSession): Unit = {

    green_println(s"save_table = ${table}")
    val sql =
      s"""
         | select *
         | from ${table}
         | where day = '${day}'
         | and ${partitionName} = '${partitionValue}'
         | limit ${logNumber}
         | """.stripMargin
    green_println(s"sql = ${sql}")

    for (item: Row <- spark.sql(sql).rdd.collect()) {
      green_println(item.toString())
    }
  }

  /** Overloaded: prints sample using a custom partition key (without saving). */
  def printHiveInfo(table: String,
                    day: String,
                    partitionName: String,
                    partitionValue: String,
                    logNumber: Int,
                    spark: SparkSession): Unit = {

    green_println(s"save_table = ${table}")
    val sql =
      s"""
         | select *
         | from ${table}
         | where day = '${day}'
         | and ${partitionName} = '${partitionValue}'
         | limit ${logNumber}
         | """.stripMargin
    green_println(s"sql = ${sql}")

    for (item: Row <- spark.sql(sql).rdd.collect()) {
      green_println(item.toString())
    }
  }


  /** Returns the most recent `day` partition value <= `today`. */
  def getRecentPartition(spark: SparkSession, table: String, today: String): String = {
    val rows = spark.sql(
      s"select day from $table where day <= cast('$today' as string) order by day desc limit 1"
    ).rdd.map { x => x.getString(0) }.take(1)
    if (rows.isEmpty) "" else rows(0)
  }


  /** Returns the latest partition value (lexicographic max) for the given table. */
  def getLatestPartition(hiveContext: SparkSession, table: String): String = {
    val partitions: Array[String] = hiveContext
      .sql(s"show partitions $table")
      .rdd
      .map(r => r.getString(0))
      .collect()

    if (partitions.isEmpty) return ""

    try {
      partitions
        .max
        .split("/")(0)
        .split("=")(1)
    } catch {
      case _: Exception => ""
    }
  }

  /** Returns `dayCurrent` or the nearest partition <= `dayCurrent` that exists. */
  def getOrNearestPartition(spark: SparkSession,
                            table: String,
                            dayCurrent: String): String = {
    val partitions = spark
      .sql(s"show partitions $table")
      .rdd
      .map(r => r.getString(0))
      .collect()
    if (partitions.isEmpty) return dayCurrent

    try {
      val days = partitions.map(r => r.split("=")(1)).filter(_ <= dayCurrent)
      if (days.isEmpty) dayCurrent else days.max
    } catch {
      case _: Exception => dayCurrent
    }
  }
}
