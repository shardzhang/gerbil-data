package utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.LogUtils.green_println

/**
 * @author Shard Zhang
 * @date 2022/4/20 16:47
 * @note
 */
object HiveUtils {
  def saveAndPrint(data: DataFrame,
                   table: String,
                   day: String,
                   logNumber: Int,
                   spark: SparkSession,
                   part: Int = 10): Unit = {
    saveToHive(data, table, day, spark, part)
    printHiveInfo(table, day, logNumber, spark)
  }

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


  // 取数据源最近的分区
  def getRecentPartition(spark: SparkSession, table: String, today: String): String = {
    val sql =
      s"""
         | select day
         | from $table
         | where day <= '$today'
         | order by day desc
         | limit 1
         |""".stripMargin
    green_println(s"sql = ${sql}")
    spark.sql(sql).rdd.map { x => x.getString(0) }.take(1)(0)
  }

  // 如何写一个接口? 实现复用?

  /** 获取数据的最近分区 */
  def getLatestPartition(hiveContext: SparkSession, table: String): String = {
    // dt=2022-05-09
    val partitions: Array[String] = hiveContext
      .sql(s"show partitions $table")
      .rdd
      .map(r => r.getString(0))
      .collect()

    if (partitions.isEmpty) return ""

    val part = try {
      val cmp = new Ordering[String] {
        override def compare(x: String, y: String): Int = {
          x.compare(y)
        }
      }

      partitions
        .max(cmp)
        .split("/")(0)
        .split("=")(1)
    } catch {
      case _: Throwable => ""
    }

    part
  }

  /** 获取距离dayCurrent最近的分区 */
  def getOrNearestPartition(spark: SparkSession,
                            table: String,
                            dayCurrent: String): String = {
    val partitions = spark
      .sql(s"show partitions $table")
      .rdd
      .map(r => r.getString(0))
      .collect()
    if (partitions.isEmpty) return dayCurrent

    val nearestPartition = try {
      partitions
        .map(r => r.split("=")(1))
        .filter(_ <= dayCurrent)
        .max
    } catch {
      case _: Throwable => dayCurrent
    }

    return nearestPartition
  }
}
