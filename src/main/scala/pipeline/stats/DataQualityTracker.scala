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

  /** Records a stage with count only (e.g. val/test splits where detailed stats are not computed). */
  def recordCounts(stage: String, totalCount: Long): Unit = {
    reports += StageQuality(stage, totalCount, None)
  }

  /** Prints all collected stage metrics in a formatted report. */
  def printReport(): Unit = {
    println("\n" + "=" * 80)
    println("DATA QUALITY REPORT")
    println("=" * 80)
    for (r <- reports) {
      println("─" * 40)
      println("Stage: " + r.stage)
      println("  Total:      " + r.totalCount)
      if (r.validCount.isDefined) {
        val v = r.validCount.get
        val pct = v * 100.0 / r.totalCount
        println("  Valid:      " + v + " (" + "%.2f".format(pct) + "%)")
      }
      if (r.targetDistribution.nonEmpty) {
        println("  Targets:    " + r.targetDistribution.size + " distinct")
        val sortedTargets = r.targetDistribution.sortWith((a, b) => a._2 > b._2)
        val top5 = sortedTargets.take(5)
        val top5Str = top5.map(t => t._1 + "=" + t._2).mkString(", ")
        println("  Top-5:      " + top5Str)
      }
    }
    println("=" * 80)
  }
}
