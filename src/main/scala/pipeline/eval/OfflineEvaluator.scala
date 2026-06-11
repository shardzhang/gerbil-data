package pipeline.eval

import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.LogUtils.green_println

/**
 * @author shard zhang
 * @date 2026/6/11 14:32
 * @note Offline evaluation — pos/neg ratio, feature coverage, sample statistics of encoded data
 */

/** Offline evaluation for featurized training data.
 * Reads TFRecord or Parquet output from ML1MPipeline and computes
 * positive/negative ratio, feature coverage, and basic sample statistics.
 */
object OfflineEvaluator {

  /** Evaluates featurized training data and prints a metrics report.
    *
    * @param dataPath  Path to TFRecord or Parquet directory (e.g. .../yesterday/tfrecord)
    * @param format    Input format: "tfrecord" or "parquet"
    * @param labelCol  Name of the label column (default "target")
    * @param labelTopK Top-K labels to show in distribution (default 10, 0 = all)
    * @param topK      Top-K features to show in coverage report (default 20)
     */
  def evaluate(dataPath: String, format: String = "parquet", labelCol: String = "target", labelTopK: Int = 10, topK: Int = 20): Unit = {
    val spark = SparkSession.active

    val df = if (format == "parquet") {
      spark.read.parquet(dataPath)
    } else if (format == "tfrecord") {
      val schema = inferSchema(spark, dataPath)
      spark.read.format("tfrecords").schema(schema).load(dataPath)
    } else {
      throw new IllegalArgumentException("Unsupported format: " + format)
    }

    val totalCount = df.count()
    if (totalCount == 0) {
      green_println("[Eval] No records found, skipping evaluation.")
      return
    }

    println("\n" + "=" * 80)
    println("OFFLINE EVALUATION REPORT")
    println("=" * 80)
    println("  Total samples:  " + totalCount)

    // Positive / negative ratio
    computeLabelDistribution(df, labelCol, labelTopK)

    // Feature coverage: for each feature column, compute non-null ratio
    computeFeatureCoverage(df, topK)

    println("=" * 80)
  }

  private def computeLabelDistribution(df: DataFrame, labelCol: String, labelTopK: Int): Unit = {
    if (!df.columns.contains(labelCol)) {
      green_println("[Eval] Label column '" + labelCol + "' not found, skipping label distribution.")
      return
    }

    // Count distinct label values and their frequencies
    val labelRows = df.groupBy(labelCol).count().collect()
    val total = labelRows.map(r => r.getLong(1)).sum

    val sortedRows = labelRows.sortBy(r => -r.getLong(1))
    val topRows = if (labelTopK > 0) sortedRows.take(labelTopK) else sortedRows
    val header = if (labelTopK > 0) s"Label distribution (top $labelTopK, $labelCol)" else s"Label distribution ($labelCol)"
    println("  " + header + ":")
    for (r <- topRows) {
      val label = r.get(0)
      val count = r.getLong(1)
      val pct = count * 100.0 / total
      println("    " + label + ": " + count + " (" + "%.2f".format(pct) + "%)")
    }
  }

  private def computeFeatureCoverage(df: DataFrame, topK: Int): Unit = {
    // Identify feature columns: *_index, *_value, *_raw
    val allCols = df.columns
    val featureCols = scala.collection.mutable.ListBuffer.empty[String]
    for (c <- allCols) {
      if ((c.endsWith("_index") || c.endsWith("_value") || c.endsWith("_raw")) && c != "target") {
        featureCols += c
      }
    }
    val selectedCols = featureCols.take(topK)

    if (selectedCols.isEmpty) {
      green_println("[Eval] No feature columns found, skipping coverage.")
      return
    }

    println("  Feature coverage (top " + selectedCols.length + "):")

    val total = df.count()
    for (colName <- selectedCols) {
      val fieldType = df.schema(colName).dataType
      val nonNullCount = df.filter(df.col(colName).isNotNull).count()
      val coverage = if (total > 0) nonNullCount * 100.0 / total else 0.0
      println("    " + colName + "  " + "%.2f".format(coverage) + "% (" + nonNullCount + " / " + total + ")")
    }
  }

  private def inferSchema(spark: SparkSession, path: String): StructType = {
    // Read a single TFRecord file to infer schema
    try {
      val df = spark.read.format("tfrecords").load(path)
      df.schema
    } catch {
      case _: Exception =>
        StructType(Seq(
          StructField("target", FloatType, nullable = true)
        ))
    }
  }

  /** CLI entry point for offline evaluation. */
  def main(args: Array[String]): Unit = {
    val usage = "Usage: OfflineEvaluator --data_path <path> [--format tfrecord|parquet] [--label_col target] [--label_top_k 10] [--top_k 20]\n" +
      "Example: OfflineEvaluator --data_path /path/to/20260610/tfrecord --format tfrecord"

    if (args.length < 2) {
      println(usage)
      sys.exit(1)
    }

    var dataPath = ""
    var format = "parquet"
    var labelCol = "target"
    var labelTopK = 10
    var topK = 20

    var i = 0
    while (i < args.length) {
      val key = args(i)
      val value = if (i + 1 < args.length) args(i + 1) else ""
      if (key == "--data_path") { dataPath = value; i += 2 }
      else if (key == "--format") { format = value; i += 2 }
      else if (key == "--label_col") { labelCol = value; i += 2 }
      else if (key == "--label_top_k") { labelTopK = value.toInt; i += 2 }
      else if (key == "--top_k") { topK = value.toInt; i += 2 }
      else {
        println("Unknown argument: " + key)
        i += 1
      }
    }

    if (dataPath.isEmpty) {
      println(usage)
      sys.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("OfflineEvaluator")
      .getOrCreate()

    try {
      evaluate(dataPath, format, labelCol, labelTopK, topK)
    } finally {
      spark.stop()
    }
  }
}
