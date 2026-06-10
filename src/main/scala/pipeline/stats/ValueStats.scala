package pipeline.stats

final class RunningValueStats(var sum: Double = 0.0D, var powerSum: Double = 0.0D, var count: Long = 0L) extends Serializable {
  def add(value: Float): RunningValueStats = {
    sum += value.toDouble
    powerSum += value.toDouble * value.toDouble
    count += 1L
    this
  }

  def merge(other: RunningValueStats): RunningValueStats = {
    sum += other.sum
    powerSum += other.powerSum
    count += other.count
    this
  }
}

final case class PosInfo(pos: Int, sum: Double = 0.0D, powerSum: Double = 0.0D, count: Long = 0L) extends Serializable {
  def mean: Double = {
    if (count <= 0L) {
      return 0.0D
    }
    sum / count.toDouble
  }

  def std: Double = {
    if (count <= 0L) {
      return 1.0D
    }
    val variance = math.max(powerSum * 1.0 / count - math.pow(mean, 2), 0.0D)
    math.sqrt(variance + 0.000001D)
  }

  def merge(stats: RunningValueStats): PosInfo = {
    PosInfo(pos, sum + stats.sum, powerSum + stats.powerSum, count + stats.count)
  }
}
