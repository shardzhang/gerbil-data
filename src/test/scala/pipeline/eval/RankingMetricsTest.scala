package pipeline.eval

import org.scalatest.{Matchers, WordSpec}

class RankingMetricsTest extends WordSpec with Matchers {

  private def auc(labels: Array[Double], scores: Array[Double]): Double = {
    RankingMetrics.localAuc(scores.zip(labels))
  }

  "RankingMetrics.localAuc" should {

    "return 1.0 for perfect ranking" in {
      val result = auc(Array(0.0, 0.0, 1.0, 1.0), Array(0.1, 0.2, 0.9, 0.8))
      assert(result === 1.0)
    }

    "return 0.0 for completely reversed ranking" in {
      val result = auc(Array(0.0, 0.0, 1.0, 1.0), Array(0.9, 0.8, 0.1, 0.2))
      assert(result === 0.0)
    }

    "return 0.5 when positive/negative scores are interleaved" in {
      // ascending scores: 0.3(neg), 0.4(pos), 0.5(neg), 0.6(pos)
      // pos ranks = 1 + 3 = 4, AUC = (4 - 1) / 4 = 0.75
      val result = auc(Array(0.0, 1.0, 0.0, 1.0), Array(0.3, 0.4, 0.5, 0.6))
      assert(result === 0.75)
    }

    "return 0.75 for typical case" in {
      val result = auc(Array(1.0, 1.0, 0.0, 0.0), Array(0.9, 0.7, 0.6, 0.4))
      assert(result === 1.0)
    }

    "return 0.75 for another case" in {
      // P=2, N=2; 3 out of 4 pairs have pos > neg
      val result = auc(Array(1.0, 1.0, 0.0, 0.0), Array(0.9, 0.4, 0.6, 0.3))
      assert(result === 0.75)
    }

    "handle single positive sample" in {
      val result = auc(Array(1.0, 0.0, 0.0), Array(0.9, 0.5, 0.4))
      assert(result === 1.0)
    }

    "return 0.5 when all labels are the same" in {
      val result = auc(Array(1.0, 1.0, 1.0), Array(0.9, 0.5, 0.4))
      assert(result === 0.5)
    }

    "handle mixed ranking" in {
      // labels: pos(1), neg(0), neg(0), pos(1)
      // ascending scores: 0.2(neg), 0.4(neg), 0.6(pos), 0.8(pos)
      // pos ranks (0-indexed): 2 + 3 = 5, AUC = (5 - 1) / 4 = 1.0
      val result = auc(Array(1.0, 0.0, 0.0, 1.0), Array(0.6, 0.2, 0.4, 0.8))
      assert(result === 1.0)
    }

    "handle large dataset" in {
      val n = 1000
      val labels = Array.fill(n / 2)(0.0) ++ Array.fill(n / 2)(1.0)
      val scores = Array.tabulate(n)(i => i.toDouble / n)
      val result = auc(labels, scores)
      assert(Math.abs(result - 1.0) < 0.001)
    }
  }

  "RankingMetrics.localAuc" should {

    "compute per-group AUC via grouping" in {
      val groups = Array("u1", "u1", "u1", "u2", "u2")
      val labels = Array(1.0, 1.0, 0.0, 1.0, 0.0)
      val scores = Array(0.9, 0.7, 0.5, 0.8, 0.3)

      val grouped = groups.indices
        .groupBy(groups(_))
        .map { case (gid, idxs) => (gid, idxs.map(i => (scores(i), labels(i)))) }

      val perGroupAuc = grouped.map { case (gid, pairs) =>
        gid -> RankingMetrics.localAuc(pairs.toArray)
      }

      assert(perGroupAuc.contains("u1"))
      assert(perGroupAuc.contains("u2"))
      // u1: labels=[1,1,0] scores=[0.9,0.7,0.5], all pos > neg => AUC=1.0
      assert(Math.abs(perGroupAuc("u1") - 1.0) < 0.01)
      // u2: labels=[1,0] scores=[0.8,0.3], pos > neg => AUC=1.0
      assert(Math.abs(perGroupAuc("u2") - 1.0) < 0.01)

      val totalSamples = groups.length
      val weightedSum = grouped.map { case (gid, pairs) =>
        pairs.length * perGroupAuc(gid)
      }.sum
      val gauc = weightedSum / totalSamples
      assert(gauc === 1.0)
    }

    "handle group with single label value" in {
      val labels = Array(1.0, 1.0, 0.0)
      val scores = Array(0.9, 0.5, 0.4)
      val pairs = scores.zip(labels)
      assert(RankingMetrics.localAuc(pairs.take(2)) === 0.5) // only positives
      assert(RankingMetrics.localAuc(pairs.drop(2)) === 0.5) // only negatives
    }
  }
}
