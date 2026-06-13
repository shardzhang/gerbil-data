package pipeline.stats

/**
 * Running statistics — online sum, sum-of-squares, count for mean/variance computation
 */

/** Tracks running sum-of-values, sum-of-squares, and count for online mean/variance computation. */
final class RunningValueStats(var sum: Double = 0.0D, var powerSum: Double = 0.0D, var count: Long = 0L) extends Serializable {
  /** Incorporates a single value into the running statistics. */
  def add(value: Float): RunningValueStats = {
    sum += value.toDouble
    powerSum += value.toDouble * value.toDouble
    count += 1L
    this
  }

  /** Merges statistics from another partition. */
  def merge(other: RunningValueStats): RunningValueStats = {
    sum += other.sum
    powerSum += other.powerSum
    count += other.count
    this
  }
}

/** Persisted position metadata for a feature value: embedding index plus distribution stats for online normalization. */
final case class PosInfo(pos: Int, sum: Double = 0.0D, powerSum: Double = 0.0D, count: Long = 0L) extends Serializable {
  /** Computes the mean of observed values. Returns 0.0 if no observations. */
  def mean: Double = {
    if (count <= 0L) {
      return 0.0D
    }
    sum / count.toDouble
  }

  /** Computes the standard deviation of observed values. Returns 1.0 if no observations. */
  def std: Double = {
    if (count <= 0L) {
      return 1.0D
    }
    val variance = math.max(powerSum * 1.0 / count - math.pow(mean, 2), 0.0D)
    math.sqrt(variance + 0.000001D)
  }

  /** Merges this position info with new statistics (e.g. from a subsequent run). */
  def merge(stats: RunningValueStats): PosInfo = {
    PosInfo(pos, sum + stats.sum, powerSum + stats.powerSum, count + stats.count)
  }
}
