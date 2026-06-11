package featurizer.core

import scala.util.Random

/**
 * @author shard zhang
 * @date 2026/6/11 11:18
 * @note Abstract negative sampler — generates unobserved (user, item) pairs for training
 */

/** Generates negative training samples by sampling unobserved (user, item) pairs.
  *
  * Three sampling strategies are supported:
  *  - random:   uniform random sampling from unobserved items
  *  - popular:  sampling weighted by item popularity (more popular → more likely to appear as negative)
  *  - mixed:    blend of random and popularity-based
  *
  * Subclasses implement [[sample()]] with Spark wiring, calling the algorithm helpers
  * from the companion object inside closures.
  */
abstract class NegativeSampler[Sample, ID] extends Serializable {

  /** Sampling strategy name: "random", "popular", or "mixed". */
  def strategy: String

  /** Random seed for reproducibility. */
  def seed: Long = 42L

  /** Whether the sample is positive (used to decide which samples to generate negatives for). */
  protected def isPositive(sample: Sample): Boolean

  /** Extracts the user ID from a sample for candidate filtering. */
  protected def extractUserId(sample: Sample): ID

  /** Generates negative samples from an existing sample pool.
    *
    * @param samples  Input RDD of (sample, isPositive) pairs
    * @param negRatio Number of negatives to generate per positive sample
    * @return         Original samples + generated negatives
    */
  def sample(samples: org.apache.spark.rdd.RDD[(Sample, Boolean)], negRatio: Int): org.apache.spark.rdd.RDD[(Sample, Boolean)]
}

object NegativeSampler {

  /** Computes candidate (unobserved) items for a user by removing history from the pool.
    *
    * @param uid     User ID
    * @param pool    All possible item IDs
    * @param history Items the user has already interacted with
    * @return        Candidate item IDs for negative sampling
    */
  def computeCandidates[ID](uid: ID, pool: Array[ID], history: Map[ID, Set[ID]]): Array[ID] = {
    val rated = history.getOrElse(uid, Set.empty[ID])
    pool.filter(id => !rated.contains(id))
  }

  /** Selects negative item IDs from candidates using the configured strategy.
    *
    * @param candidates  Candidate item IDs (unobserved by the user)
    * @param popCounts   Popularity counts per item (used by "popular" and "mixed" strategies)
    * @param numNeg      Number of negatives to select
    * @param strategy    Sampling strategy: "random", "popular", or "mixed"
    * @param rand        Random number generator
    * @return            Selected negative item IDs
    */
  def selectNegativeItems[ID](
    candidates: Array[ID],
    popCounts: Map[ID, Int],
    numNeg: Int,
    strategy: String,
    rand: Random
  ): Seq[ID] = {
    if (candidates.isEmpty) return Seq.empty
    strategy match {
      case "random" =>
        rand.shuffle(candidates.toSeq).take(numNeg)
      case _ => // "popular" or "mixed"
        val half = if (strategy == "mixed") numNeg / 2 else numNeg
        val weighted = candidates.map { id =>
          (id, math.pow(popCounts.getOrElse(id, 1).toDouble, 0.75))
        }
        val popular = (1 until half).map { _ =>
          var r = rand.nextDouble() * weighted.map(_._2).sum
          weighted.find { case (_, w) => r -= w; r <= 0 } match {
            case Some((id, _)) => id
            case None => weighted.last._1
          }
        }.distinct.toSeq
        if (strategy == "mixed") {
          val randomPart = rand.shuffle(candidates.toSeq).take(numNeg - popular.size)
          (randomPart ++ popular).distinct.take(numNeg)
        } else {
          popular.take(numNeg)
        }
    }
  }
}
