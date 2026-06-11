package processing.stats

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{NumericType, StringType => SparkStringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.LogUtils.green_println

/**
 * @author shard zhang
 * @date 2026/6/11 14:32
 * @note Data quality checks — numeric column summary statistics and validation
 */

/** Summary statistics for a numeric column. */
case class NumericColStats(
  /** Minimum value (null if column is all-null). */
  min: Option[Double] = None,
  /** Maximum value. */
  max: Option[Double] = None,
  /** Arithmetic mean. */
  mean: Option[Double] = None,
  /** Sample standard deviation. */
  stddev: Option[Double] = None
)

/** Quality metrics for a single column. */
case class ColumnQuality(
  /** Column name. */
  name: String,
  /** Spark SQL type name (e.g. "string", "integer"). */
  dataType: String,
  /** Number of rows where this column is NULL. */
  nullCount: Long,
  /** nullCount / totalCount. */
  nullRatio: Double,
  /** Distinct count (string columns only). */
  cardinality: Option[Long] = None,
  /** Numeric stats (numeric columns only). */
  numericStats: Option[NumericColStats] = None
)

/** Snapshot of quality metrics for one ETL stage. */
case class QualityReport(
  /** Stage name (e.g. "clean_sample", "join_sample"). */
  stage: String,
  /** Total records at this stage. */
  totalCount: Long,
  /** Per-column quality metrics. */
  columns: Seq[ColumnQuality]
) {
  /** Prints a formatted quality report to stdout. */
  def print(): Unit = {
    println("\n" + "=" * 80)
    println("DATA QUALITY CHECK: " + stage)
    println("=" * 80)
    println("  Total records:  " + totalCount)
    for (c <- columns) {
      println("  ─ " + c.name + " (" + c.dataType + ")")
      if (c.nullRatio > 0) {
        println("    NULL:       " + c.nullCount + " (" + "%.2f".format(c.nullRatio * 100) + "%)")
      } else {
        println("    NULL:       0 (0.00%)")
      }
      if (c.cardinality.isDefined) {
        println("    Cardinality: " + c.cardinality.get)
      }
      if (c.numericStats.isDefined) {
        val s = c.numericStats.get
        if (s.min.isDefined)   { println("    Min:        " + s.min.get) }
        if (s.max.isDefined)   { println("    Max:        " + s.max.get) }
        if (s.mean.isDefined)  { println("    Mean:       " + "%.4f".format(s.mean.get)) }
        if (s.stddev.isDefined) { println("    Stddev:     " + "%.4f".format(s.stddev.get)) }
      }
    }
    println("=" * 80)
  }
}

/** Reusable quality checker for DataFrame-based ETL steps.
  * Computes null ratios, cardinality, numeric stats and persists results
  * for cross-run drift detection.
  */
object DataQualityChecker {

  /** Computes and prints quality metrics for a DataFrame, then persists stats for drift detection. */
  def check(df: DataFrame, stage: String, outputPath: String): QualityReport = {
    val totalCount = df.count()
    if (totalCount == 0) {
      val empty = QualityReport(stage, 0, Seq.empty)
      empty.print()
      return empty
    }

    val schema = df.schema
    val nullCounts = computeNullCounts(df, schema)
    val cardinalities = computeCardinalities(df, schema)
    val numericStats = computeNumericStats(df, schema)

    val columns = for (field <- schema.fields) yield {
      ColumnQuality(
        name = field.name,
        dataType = field.dataType.simpleString,
        nullCount = nullCounts.getOrElse(field.name, 0L),
        nullRatio = if (totalCount > 0) nullCounts.getOrElse(field.name, 0L).toDouble / totalCount else 0.0,
        cardinality = if (field.dataType.isInstanceOf[SparkStringType]) cardinalities.get(field.name) else None,
        numericStats = numericStats.get(field.name)
      )
    }

    val report = QualityReport(stage, totalCount, columns)
    report.print()
    detectDrift(df.sparkSession, report, outputPath)
    saveStats(report, outputPath)
    report
  }

  // ────────────────────────────── stats computation ──────────────────────────────

  /** Returns per-column null counts via single SQL pass. */
  private def computeNullCounts(df: DataFrame, schema: StructType): Map[String, Long] = {
    val cols = schema.fields.map(_.name)
    val selects = cols.map(c => "SUM(IF(`" + c + "` IS NULL, 1, 0)) AS `" + c + "`").mkString(", ")
    df.createOrReplaceTempView("_quality_null_tmp")
    val row = df.sparkSession.sql("SELECT " + selects + " FROM _quality_null_tmp").first()
    val result = scala.collection.mutable.Map.empty[String, Long]
    for (colName <- cols) {
      val idx = row.fieldIndex(colName)
      result(colName) = row.getLong(idx)
    }
    result.toMap
  }

  /** Returns distinct-count per string column. */
  private def computeCardinalities(df: DataFrame, schema: StructType): Map[String, Long] = {
    val stringFields = schema.fields.filter(_.dataType.isInstanceOf[SparkStringType])
    if (stringFields.isEmpty) return Map.empty

    val cols = stringFields.map(_.name)
    val selects = cols.map(c => "COUNT(DISTINCT `" + c + "`) AS `" + c + "`").mkString(", ")
    df.createOrReplaceTempView("_quality_card_tmp")
    val row = df.sparkSession.sql("SELECT " + selects + " FROM _quality_card_tmp").first()
    val result = scala.collection.mutable.Map.empty[String, Long]
    for (colName <- cols) {
      val idx = row.fieldIndex(colName)
      result(colName) = row.getLong(idx)
    }
    result.toMap
  }

  /** Returns min/max/mean/stddev per numeric column. */
  private def computeNumericStats(df: DataFrame, schema: StructType): Map[String, NumericColStats] = {
    val numericFields = schema.fields.filter(_.dataType.isInstanceOf[NumericType])
    if (numericFields.isEmpty) return Map.empty

    df.createOrReplaceTempView("_quality_num_tmp")
    val result = scala.collection.mutable.Map.empty[String, NumericColStats]
    for (field <- numericFields) {
      val sqlStmt = "SELECT MIN(`" + field.name + "`) AS min, MAX(`" + field.name + "`) AS max, " +
        "AVG(`" + field.name + "`) AS avg, STDDEV(`" + field.name + "`) AS std FROM _quality_num_tmp"
      val row = df.sparkSession.sql(sqlStmt).first()

      var vMin: Option[Double] = None
      var vMax: Option[Double] = None
      var vAvg: Option[Double] = None
      var vStd: Option[Double] = None
      if (row.get(0) != null) { vMin = Some(row.get(0).toString.toDouble) }
      if (row.get(1) != null) { vMax = Some(row.get(1).toString.toDouble) }
      if (row.get(2) != null) { vAvg = Some(row.get(2).toString.toDouble) }
      if (row.get(3) != null) { vStd = Some(row.get(3).toString.toDouble) }

      result(field.name) = NumericColStats(min = vMin, max = vMax, mean = vAvg, stddev = vStd)
    }
    result.toMap
  }

  // ────────────────────────────── persistence ──────────────────────────────

  /** Parent directory for all quality stats (parent_of_output/_quality/). */
  private def qualityDir(outputPath: String): Path = {
    val parent = new Path(outputPath).getParent.toString
    new Path(parent, "_quality")
  }

  /** Full path to the stats JSON file for a given stage. */
  private def statsFilePath(outputPath: String, stage: String): Path = {
    new Path(qualityDir(outputPath), stage + "_stats.json")
  }

  /** Persists quality metrics as columnar JSON for cross-run drift comparison.
    * Schema: stage, total_count, name, type, null_count, null_ratio, cardinality, min, max, mean, stddev
    */
  private def saveStats(report: QualityReport, outputPath: String): Unit = {
    try {
      val spark = SparkSession.active
      val path = statsFilePath(outputPath, report.stage)
      val fs = FileSystem.get(path.toUri, spark.sparkContext.hadoopConfiguration)
      fs.mkdirs(qualityDir(outputPath))

      // Build rows: one per column, with all stats as flat fields
      val rows = scala.collection.mutable.ListBuffer.empty[Row]
      for (c <- report.columns) {
        val minVal: String  = if (c.numericStats.isDefined && c.numericStats.get.min.isDefined)    { c.numericStats.get.min.get.toString } else { null }
        val maxVal: String  = if (c.numericStats.isDefined && c.numericStats.get.max.isDefined)    { c.numericStats.get.max.get.toString } else { null }
        val meanVal: String = if (c.numericStats.isDefined && c.numericStats.get.mean.isDefined)   { c.numericStats.get.mean.get.toString } else { null }
        val stdVal: String  = if (c.numericStats.isDefined && c.numericStats.get.stddev.isDefined) { c.numericStats.get.stddev.get.toString } else { null }
        val cardVal: java.lang.Long = if (c.cardinality.isDefined) { c.cardinality.get } else { null }
        rows += Row(
          report.stage, report.totalCount, c.name, c.dataType,
          c.nullCount, c.nullRatio, cardVal,
          minVal, maxVal, meanVal, stdVal)
      }

      val outputSchema = StructType(Seq(
        StructField("stage", org.apache.spark.sql.types.StringType, nullable = true),
        StructField("total_count", org.apache.spark.sql.types.LongType, nullable = true),
        StructField("name", org.apache.spark.sql.types.StringType, nullable = true),
        StructField("type", org.apache.spark.sql.types.StringType, nullable = true),
        StructField("null_count", org.apache.spark.sql.types.LongType, nullable = true),
        StructField("null_ratio", org.apache.spark.sql.types.DoubleType, nullable = true),
        StructField("cardinality", org.apache.spark.sql.types.LongType, nullable = true),
        StructField("min", org.apache.spark.sql.types.StringType, nullable = true),
        StructField("max", org.apache.spark.sql.types.StringType, nullable = true),
        StructField("mean", org.apache.spark.sql.types.StringType, nullable = true),
        StructField("stddev", org.apache.spark.sql.types.StringType, nullable = true)
      ))

      spark.createDataFrame(spark.sparkContext.parallelize(rows), outputSchema)
        .repartition(1).write.mode("overwrite").json(path.toString)
    } catch {
      case e: Exception =>
        green_println("[Quality] Failed to save quality stats: " + e.getMessage)
    }
  }

  // ────────────────────────────── drift detection ──────────────────────────────

  /** Compares current run stats against previous run, printing warnings for significant changes.
    *
    * Drift thresholds:
    *   - total count: ±20%                           → WARN
    *   - column null ratio: absolute change > 5%     → WARN
    *   - numeric mean: relative change > 50%         → WARN
    */
  private def detectDrift(spark: SparkSession, curr: QualityReport, outputPath: String): Unit = {
    val path = statsFilePath(outputPath, curr.stage)
    val fs = FileSystem.get(path.toUri, spark.sparkContext.hadoopConfiguration)

    if (!fs.exists(path)) {
      green_println("[Drift] No previous stats at " + path + ", skipping drift detection.")
      return
    }

    green_println("[Drift] Comparing " + curr.stage + " against " + path + " ...")

    val prevDf = spark.read.json(path.toString).cache()

    // Read total count from previous run
    val prevCountRow = prevDf.select("total_count").distinct().first()
    val prevTotalCount = prevCountRow.getLong(0)

    // Check total count drift
    val maxPrev = if (prevTotalCount > 0) prevTotalCount else 1L
    val countRatio = math.abs(curr.totalCount - prevTotalCount).toDouble / maxPrev.toDouble
    if (countRatio > 0.2) {
      green_println("[Drift WARN] " + curr.stage + " total count: " + prevTotalCount +
        " -> " + curr.totalCount + " (" + "%.1f".format(countRatio * 100) + "% change)")
    } else {
      green_println("[Drift OK]   " + curr.stage + " total count: " + prevTotalCount + " -> " + curr.totalCount)
    }

    // Build a lookup map from previous stats: name -> (null_ratio, mean)
    // Collect all rows and organize by column name
    val prevSchema = prevDf.schema
    val hasMean = prevSchema.fieldNames.contains("mean")
    val prevRows = prevDf.collect()
    val prevLookup = scala.collection.mutable.Map.empty[String, (Double, String)]
    for (r <- prevRows) {
      val name = r.getString(r.fieldIndex("name"))
      val nullRatio = r.getDouble(r.fieldIndex("null_ratio"))
      val meanStr = if (hasMean) { if (r.isNullAt(prevSchema.fieldIndex("mean"))) "" else r.getString(prevSchema.fieldIndex("mean")) } else ""
      prevLookup(name) = (nullRatio, meanStr)
    }

    // Check per-column drift
    for (c <- curr.columns) {
      if (prevLookup.contains(c.name)) {
        val prevData = prevLookup(c.name)
        val prevNullRatio = prevData._1
        val prevMeanStr = prevData._2

        // Check null ratio drift
        val nullDelta = math.abs(c.nullRatio - prevNullRatio)
        if (nullDelta > 0.05) {
          green_println("[Drift WARN] " + curr.stage + "." + c.name + " null ratio: " +
            "%.1f".format(prevNullRatio * 100) + "% -> " + "%.1f".format(c.nullRatio * 100) + "%")
        }

        // Check numeric mean drift
        if (c.numericStats.isDefined && c.numericStats.get.mean.isDefined) {
          val currMean = c.numericStats.get.mean.get
          if (prevMeanStr != null && prevMeanStr != "") {
            val prevMean = prevMeanStr.toDouble
            if (prevMean != 0 && math.abs((currMean - prevMean) / prevMean) > 0.5) {
              green_println("[Drift WARN] " + curr.stage + "." + c.name + " mean: " + prevMean + " -> " + currMean)
            }
          }
        }
      }
    }

    prevDf.unpersist()
  }
}
