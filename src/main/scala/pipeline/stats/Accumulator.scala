package pipeline.stats

/**
 * Online statistics accumulators for distributed feature engineering.
 *
 * Provides two interchangeable single-pass algorithms (SOS and Welford) for computing
 * per-feature-value mean and variance, alongside a persistence-oriented PosInfo wrapper.
 * Both accumulators expose identical Add and Merge primitives, enabling runtime
 * interchange within the Spark mapPartitions/reduceByKey pipeline.
 *
 * @see "Benchmarking Online Variance Algorithms for Production Feature Engineering Systems"
 */

/** Accumulator trait with online update, distributed merge, and PosInfo conversion primitives.
 *  Concrete implementations SOSAccumulator and WelfordAccumulator are interchangeable:
 *  the pipeline selects which to instantiate via a factory method. */
trait Accumulator extends Serializable {
  def add(value: Float): Accumulator
  def merge(other: Accumulator): Accumulator
  def count: Long
  def toPosInfo(pos: Int): PosInfo
  def mergeIntoPosInfo(existing: PosInfo, pos: Int): PosInfo
}

/**
 * SOS (Sum of Squares) accumulator — Algorithm 1.
 *
 * Maintains {count, sum, powerSum}. Merge is element-wise addition.
 * Variance derived as max(Q/n − μ², 0) with an epsilon guard.
 * Suffers catastrophic cancellation when μ/σ ≫ 1.
 */
final class SOSAccumulator(var sum: Double = 0.0, var powerSum: Double = 0.0, var count: Long = 0L)
  extends Accumulator {

  def add(value: Float): Accumulator = {
    val v = value.toDouble
    sum += v
    powerSum += v * v
    count += 1L
    this
  }

  def merge(other: Accumulator): Accumulator = {
    val o = other.asInstanceOf[SOSAccumulator]
    sum += o.sum
    powerSum += o.powerSum
    count += o.count
    this
  }

  def mean: Double = if (count <= 0L) 0.0 else sum / count.toDouble

  def std: Double = {
    if (count <= 0L) return 1.0
    val variance = math.max(powerSum / count.toDouble - mean * mean, 0.0)
    math.sqrt(variance + 0.000001)
  }

  def toPosInfo(pos: Int): PosInfo =
    PosInfo(pos, sum, powerSum, count, 0.0, 0.0)

  def mergeIntoPosInfo(existing: PosInfo, pos: Int): PosInfo =
    existing.copy(pos = pos).merge(this)
}

/**
 * Welford's online accumulator — Algorithm 2.
 *
 * Maintains {count, runningMean, M₂} where M₂ = Σ(xᵢ − x̄)².
 * Avoids catastrophic cancellation by computing variance through deviations
 * (xᵢ − x̄) that are on the order of σ. Merge uses Chan's pairwise formula
 * (Chan et al., 1982).
 */
final class WelfordAccumulator(var count: Long = 0L, var runningMean: Double = 0.0, var m2: Double = 0.0)
  extends Accumulator {

  def add(value: Float): Accumulator = {
    count += 1L
    val x = value.toDouble
    val delta = x - runningMean
    runningMean += delta / count.toDouble
    val deltaPrime = x - runningMean
    m2 += delta * deltaPrime
    this
  }

  /** Chan's pairwise merge formula. Equivalent to processing all elements sequentially
   *  up to floating-point reassociation order. */
  def merge(other: Accumulator): Accumulator = {
    val o = other.asInstanceOf[WelfordAccumulator]
    val nAB = count + o.count
    val delta = o.runningMean - runningMean
    runningMean = (count.toDouble * runningMean + o.count.toDouble * o.runningMean) / nAB.toDouble
    m2 = m2 + o.m2 + (count.toDouble * o.count.toDouble / nAB.toDouble) * delta * delta
    count = nAB
    this
  }

  def mean: Double = if (count <= 0L) 0.0 else runningMean

  def std: Double = {
    if (count <= 0L) return 1.0
    val variance = m2 / count.toDouble
    math.sqrt(math.max(variance, 0.0) + 0.000001)
  }

  def toPosInfo(pos: Int): PosInfo =
    PosInfo.fromWelford(pos, runningMean, m2, count)

  def mergeIntoPosInfo(existing: PosInfo, pos: Int): PosInfo =
    existing.copy(pos = pos).merge(this)
}

/**
 * Persisted position metadata for a feature value.
 *
 * Stores both SOS and Welford accumulator state to support incremental
 * vocabulary updates. Mean and standard deviation are always derived from
 * the SOS fields (sum/powerSum/count) for backward compatibility;
 * the Welford fields act as secondary state for Welford-based merges.
 */
case class PosInfo(
                    pos: Int,
                    sum: Double = 0.0,
                    powerSum: Double = 0.0,
                    count: Long = 0L,
                    welfordMean: Double = 0.0,
                    welfordM2: Double = 0.0
                  ) extends Serializable {

  def mean: Double = if (count <= 0L) 0.0 else sum / count.toDouble

  def std: Double = {
    if (count <= 0L) return 1.0
    val variance = math.max(powerSum / count.toDouble - mean * mean, 0.0)
    math.sqrt(variance + 0.000001)
  }

  def merge(stat: SOSAccumulator): PosInfo = copy(
    sum = sum + stat.sum,
    powerSum = powerSum + stat.powerSum,
    count = count + stat.count
  )

  def merge(stat: WelfordAccumulator): PosInfo = {
    if (count == 0L) {
      PosInfo(
        pos,
        stat.runningMean * stat.count.toDouble,
        stat.m2 + stat.count.toDouble * stat.runningMean * stat.runningMean,
        stat.count,
        stat.runningMean,
        stat.m2
      )
    } else {
      val (wMean, wM2) = if (count > 0L && welfordMean == 0.0 && welfordM2 == 0.0) {
        val m = sum / count.toDouble
        (m, math.max(powerSum - count.toDouble * m * m, 0.0))
      } else {
        (welfordMean, welfordM2)
      }
      val nAB = count + stat.count
      val delta = stat.runningMean - wMean
      val newM2 = wM2 + stat.m2 + (count.toDouble * stat.count.toDouble / nAB.toDouble) * delta * delta
      val newMean = (count.toDouble * wMean + stat.count.toDouble * stat.runningMean) / nAB.toDouble
      val newSum = newMean * nAB.toDouble
      val newPowerSum = newM2 + nAB.toDouble * newMean * newMean
      PosInfo(pos, newSum, newPowerSum, nAB, newMean, newM2)
    }
  }
}

object PosInfo {
  def fromSOS(pos: Int, sum: Double, powerSum: Double, count: Long): PosInfo =
    PosInfo(pos, sum, powerSum, count, 0.0, 0.0)

  def fromWelford(pos: Int, runningMean: Double, m2: Double, n: Long): PosInfo = {
    if (n <= 0L) PosInfo(pos, 0.0, 0.0, 0L, 0.0, 0.0)
    else {
      val derivedSum = runningMean * n.toDouble
      val derivedPowerSum = m2 + n.toDouble * runningMean * runningMean
      PosInfo(pos, derivedSum, derivedPowerSum, n, runningMean, m2)
    }
  }
}
