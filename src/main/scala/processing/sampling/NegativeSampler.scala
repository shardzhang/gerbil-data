package processing.sampling

import scala.util.Random

/** ETL-layer negative sampler base class.
 *
 * Provides the candidate-computation and strategy-selection algorithm as pure
 * companion-object helpers, and defines the interface that dataset-specific
 * subclasses must implement (user-ID extraction, row construction, etc.).
 *
 * Three strategies are supported:
 *  - random:   uniform random sampling from unobserved items
 *  - popular:  sampling weighted by item popularity (roulette-wheel selection)
 *  - mixed:    blend of random + popularity-based sampling (half each)
 */
abstract class NegativeSampler[T] extends Serializable {

  /** Sampling strategy: "random", "popular", or "mixed". */
  def strategy: String

  /** Random seed for reproducibility. */
  def seed: Long = 42L

  /** Extracts the user ID from a row. */
  protected def extractUserId(row: T): String

  /** Extracts the current (positive) item ID from a row. */
  protected def extractCurrentItemId(row: T): String

  /** Builds a synthetic negative row from a positive row + negative item ID + its feature JSON. */
  protected def buildNegativeRow(pos: T, negId: String, negFeatures: String): T
}

object NegativeSampler {
  /** Computes candidate (unobserved) items for a user, excluding the current positive item. */
  def computeCandidates[ID](uid: ID, currentItemId: ID, pool: Array[ID], history: Map[ID, Set[ID]]): Array[ID] = {
    val rated = history.getOrElse(uid, Set.empty[ID])
    pool.filter(id => id != currentItemId && !rated.contains(id))
  }

  /** Selects negative item IDs from candidates using the configured strategy.
   *
   * @param candidates Candidate item IDs
   * @param popCounts  Popularity counts (used by "popular" and "mixed" strategies)
   * @param numNeg     Number of negatives to select
   * @param strategy   Sampling strategy: "random", "popular", or "mixed"
   * @param rand       Random number generator
   */
  def selectNegativeItems[T](candidates: Array[T],
                             popCounts: Map[T, Int],
                             numNeg: Int,
                             strategy: String,
                             rand: Random): Seq[T] = {
    if (candidates.isEmpty) {
      return Seq.empty
    }

    strategy match {
      case "random" =>
        rand.shuffle(candidates.toSeq).distinct.take(numNeg)

      case "mixed" =>
        val half = numNeg / 2
        val popular = rouletteSample(candidates, popCounts, half, rand)
        val randomPart = rand.shuffle(candidates.toSeq).take(numNeg - popular.size)
        (randomPart ++ popular).distinct.take(numNeg)

      case _ => // "popular"
        rouletteSample(candidates, popCounts, numNeg, rand)
    }
  }

  // 轮盘赌加权随机采样，按物品权重高低做概率抽取
  private def rouletteSample[T](candidates: Array[T], popCounts: Map[T, Int], numNeg: Int, rand: Random): Seq[T] = {
    val weighted = candidates.map { id =>
      (id, math.pow(popCounts.getOrElse(id, 1).toDouble, 0.75))
    }
    (1 to numNeg)
      .map { _ =>
        var r = rand.nextDouble() * weighted.map(_._2).sum
        weighted.find {
          case (_, w) =>
            r -= w
            r <= 0
        } match {
          case Some((id, _)) => id
          case None => weighted.last._1
        }
      }.distinct
  }
}
