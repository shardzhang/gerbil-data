package pipeline.eval

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import utils.LogUtils.green_println

/**
 * Spark-based AUC / GAUC evaluation for model prediction results.
 *
 * Reads prediction data (Parquet, CSV, or TFRecord) from a given path,
 * computes binary AUC and per-user GAUC, and prints a formatted report.
 *
 * Required columns:
 *   - label column (default "target"): binary float (0.0 / 1.0)
 *   - score column (default "score"):  prediction score
 *   - group column (default "user_id"): used for GAUC grouping
 */
object SparkRankingMetrics {

  /**
   * Computes binary AUC from a DataFrame via RankingMetrics.localAuc.
   */
  def computeAuc(df: DataFrame, labelCol: String, scoreCol: String): Double = {
    val rows = df.select(scoreCol, labelCol).collect()
    val pairs = rows.map(r => (r.getDouble(0), r.getDouble(1)))
    RankingMetrics.localAuc(pairs)
  }

  /**
   * Computes Group AUC from a DataFrame using RankingMetrics.localAuc per group.
   *
   * Each group (typically a user) gets its own AUC; the final GAUC is a
   * weighted average by group sample count.
   */
  def computeGauc(df: DataFrame, groupCol: String, labelCol: String, scoreCol: String): (Double, Map[String, Double]) = {
    val rows = df.select(groupCol, scoreCol, labelCol).collect()
    val grouped = rows.map(r => (r.getString(0), (r.getDouble(1), r.getDouble(2))))
      .groupBy(_._1)
      .map { case (gid, arr) => (gid, arr.map(_._2)) }

    val perGroupAuc = grouped.map { case (gid, pairs) =>
      gid -> RankingMetrics.localAuc(pairs)
    }

    val totalSamples = rows.length
    val weightedSum = grouped.map { case (gid, pairs) =>
      pairs.length * perGroupAuc(gid)
    }.sum
    val gaucValue = if (totalSamples > 0) weightedSum / totalSamples else 0.0

    (gaucValue, perGroupAuc.toMap)
  }

  /**
   * Full evaluation: prints AUC, GAUC, positive/negative ratio, and top groups.
   */
  def evaluate(dataPath: String, format: String = "parquet",
               labelCol: String = "target", scoreCol: String = "score",
               groupCol: String = "user_id", topK: Int = 10): Unit = {
    val spark = SparkSession.active

    val df = format match {
      case "parquet" => spark.read.parquet(dataPath)
      case "csv"     => spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)
      case "tfrecord" =>
        val schema = StructType(Seq(
          StructField(labelCol, FloatType, nullable = true),
          StructField(scoreCol, FloatType, nullable = true)
        ))
        spark.read.format("tfrecords").schema(schema).load(dataPath)
      case _ => throw new IllegalArgumentException("Unsupported format: " + format)
    }

    val totalCount = df.count()
    if (totalCount == 0) {
      green_println("[RankingMetrics] No records found, skipping evaluation.")
      return
    }

    println("\n" + "=" * 80)
    println("RANKING METRICS EVALUATION")
    println("=" * 80)
    println("  Total samples:  " + totalCount)

    if (df.columns.contains(labelCol) && df.columns.contains(scoreCol)) {
      val aucVal = computeAuc(df, labelCol, scoreCol)
      println(f"  AUC:            $aucVal%.6f")

      if (df.columns.contains(groupCol)) {
        val (gaucVal, perGroup) = computeGauc(df, groupCol, labelCol, scoreCol)
        println(f"  GAUC:           $gaucVal%.6f  (grouped by $groupCol)")
        println(f"  Groups:         ${perGroup.size}")

        val sortedGroups = perGroup.toSeq.sortBy(-_._2)
        println(s"  Top-$topK groups by AUC:")
        for ((gid, aucG) <- sortedGroups.take(topK)) {
          println(f"    $gid%s  $aucG%.6f")
        }
      }
    } else {
      green_println(s"[RankingMetrics] Required columns not found: $labelCol, $scoreCol")
      green_println(s"  Available columns: ${df.columns.mkString(", ")}")
    }
    println("=" * 80)
  }

  /** CLI entry point. */
  def main(args: Array[String]): Unit = {
    val usage =
      """Usage: SparkRankingMetrics --data_path <path>
        |  [--format parquet|csv|tfrecord]  (default: parquet)
        |  [--label_col target]             (default: target)
        |  [--score_col score]              (default: score)
        |  [--group_col user_id]            (default: user_id)
        |  [--top_k 10]                     (default: 10)
        |Example: SparkRankingMetrics --data_path /path/to/predictions --format parquet
        |""".stripMargin

    if (args.length < 2) {
      println(usage)
      sys.exit(1)
    }

    var dataPath = ""
    var format = "parquet"
    var labelCol = "target"
    var scoreCol = "score"
    var groupCol = "user_id"
    var topK = 10

    var i = 0
    while (i < args.length) {
      val key = args(i)
      val value = if (i + 1 < args.length) args(i + 1) else ""
      if (key == "--data_path") { dataPath = value; i += 2 }
      else if (key == "--format") { format = value; i += 2 }
      else if (key == "--label_col") { labelCol = value; i += 2 }
      else if (key == "--score_col") { scoreCol = value; i += 2 }
      else if (key == "--group_col") { groupCol = value; i += 2 }
      else if (key == "--top_k") { topK = value.toInt; i += 2 }
      else { println("Unknown argument: " + key); i += 1 }
    }

    if (dataPath.isEmpty) {
      println(usage)
      sys.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("SparkRankingMetrics")
      .getOrCreate()

    try {
      evaluate(dataPath, format, labelCol, scoreCol, groupCol, topK)
    } finally {
      spark.stop()
    }
  }
}
