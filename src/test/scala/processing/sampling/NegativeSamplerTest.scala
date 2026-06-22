package processing.sampling

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class NegativeSamplerTest extends WordSpec with Matchers {

  private val rand = new Random(42L)

  "NegativeSampler.computeCandidates" should {
    "exclude current item and rated items" in {
      val pool = Array("1", "2", "3", "4", "5")
      val history = Map("u1" -> Set("2", "4"))
      val candidates = NegativeSampler.computeCandidates("u1", "3", pool, history)
      assert(candidates.toSeq === Seq("1", "5"))
    }

    "return all pool items when history is empty and no positive item" in {
      val pool = Array("1", "2", "3")
      val candidates = NegativeSampler.computeCandidates("u1", "0", pool, Map.empty[String, Set[String]])
      assert(candidates.toSeq === Seq("1", "2", "3"))
    }

    "return empty when all items are rated or current" in {
      val pool = Array("1", "2", "3")
      val history = Map("u1" -> Set("1", "2"))
      val candidates = NegativeSampler.computeCandidates("u1", "3", pool, history)
      assert(candidates.isEmpty)
    }

    "handle empty pool" in {
      val candidates = NegativeSampler.computeCandidates("u1", "1", Array.empty[String], Map.empty[String, Set[String]])
      assert(candidates.isEmpty)
    }
  }

  "NegativeSampler.selectNegativeItems" should {
    val pool = Array("1", "2", "3", "4", "5")
    val popCounts = Map("1" -> 100, "2" -> 50, "3" -> 10, "4" -> 5, "5" -> 1)

    "return empty when candidates is empty" in {
      val result = NegativeSampler.selectNegativeItems(Array.empty, popCounts, 3, "random", rand)
      assert(result.isEmpty)
    }

    "sample with random strategy" in {
      val result = NegativeSampler.selectNegativeItems(pool, popCounts, 3, "random", rand)
      assert(result.size === 3)
      assert(result.forall(pool.contains))
    }

    "sample with popular strategy" in {
      val result = NegativeSampler.selectNegativeItems(pool, popCounts, 3, "popular", rand)
      assert(result.nonEmpty)
      assert(result.size <= 3)
      assert(result.forall(pool.contains))
    }

    "sample with mixed strategy" in {
      val result = NegativeSampler.selectNegativeItems(pool, popCounts, 4, "mixed", rand)
      assert(result.nonEmpty)
      assert(result.size <= 4)
      assert(result.forall(pool.contains))
    }

    "respect numNeg" in {
      val result = NegativeSampler.selectNegativeItems(pool, popCounts, 1, "random", rand)
      assert(result.size === 1)
    }

    "deduplicate results" in {
      // With numNeg larger than pool size, should still deduplicate
      val result = NegativeSampler.selectNegativeItems(pool, popCounts, 10, "random", rand)
      assert(result.size === pool.length)
    }

    "popular strategy favors high-pop items" in {
      // Run popular sampling many times, "1" (pop=100) should appear more often
      val counts = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
      for (_ <- 1 to 100) {
        val result = NegativeSampler.selectNegativeItems(pool, popCounts, 1, "popular", new Random)
        counts(result.head) += 1
      }
      assert(counts("1") > counts("5")) // highest pop should appear more than lowest
    }
  }
}
