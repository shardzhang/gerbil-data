package pipeline.stats

/** Quality snapshot for one ETL stage: record count, parse success rate, and target label distribution. */
case class StageQuality(
  /** Stage name (e.g. "train_stats", "val_split"). */
  stage: String,
  /** Total records (including parse failures) at this stage. */
  totalCount: Long,
  /** Count of successfully parsed records. None if only count is tracked. */
  validCount: Option[Long] = None,
  /** Sorted (targetId, count) pairs for target distribution snapshot. */
  targetDistribution: Seq[(Int, Int)] = Seq.empty
)

/** Collects quality metrics across pipeline stages and prints a consolidated report at the end. */
class DataQualityTracker {
  /** Accumulated stage quality snapshots. */
  private val reports = scala.collection.mutable.ListBuffer.empty[StageQuality]

  /** Records a stage with full stats: parse success count and target distribution. */
  def record(stage: String, totalCount: Long, validCount: Long, targetDistribution: Seq[(Int, Int)]): Unit = {
    reports += StageQuality(stage, totalCount, Some(validCount), targetDistribution)
  }

  /** Records a stage with count only (e.g. val/test splits where detailed stats aren't computed). */
  def recordCounts(stage: String, totalCount: Long): Unit = {
    reports += StageQuality(stage, totalCount, None)
  }

  /** Prints all collected stage metrics in a formatted report. */
  def printReport(): Unit = {
    println("\n" + "=" * 80)
    println("DATA QUALITY REPORT")
    println("=" * 80)
    reports.foreach { r =>
      println("─" * 40)
      println(s"Stage: ${r.stage}")
      println(f"  Total:      ${r.totalCount}%,d")
      r.validCount.foreach { v =>
        println(f"  Valid:      ${v}%,d (${v * 100.0 / r.totalCount}%.2f%%)")
      }
      if (r.targetDistribution.nonEmpty) {
        println(s"  Targets:    ${r.targetDistribution.size} distinct")
        val top5 = r.targetDistribution.sortBy(-_._2).take(5)
        println(s"  Top-5:      ${top5.map { case (k, v) => s"$k=$v" }.mkString(", ")}")
      }
    }
    println("=" * 80)
  }
}
