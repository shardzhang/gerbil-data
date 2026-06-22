package featurizer.ml1m

import org.scalatest.{Matchers, WordSpec}

class ML1MTargetsTest extends WordSpec with Matchers {

  private def sample(target: Int, rating: Float): ML1MSample = {
    val s = new ML1MSample()
    s.target = target
    s.rating = rating
    s
  }

  "Target" should {
    "parse target from sample" in {
      val t = new Target()
      t.parse(sample(5, 4.0F))
      assert(t.target === 5)
    }

    "parse target 1" in {
      val t = new Target()
      t.parse(sample(1, 3.0F))
      assert(t.target === 1)
    }
  }

  "Label" should {
    "return 1 for rating > 3" in {
      val l = new Label()
      l.parse(sample(1, 4.0F))
      assert(l.target === 1.0F)
    }

    "return 0 for rating = 3" in {
      val l = new Label()
      l.parse(sample(1, 3.0F))
      assert(l.target === 0.0F)
    }

    "return 0 for rating < 3" in {
      val l = new Label()
      l.parse(sample(1, 2.0F))
      assert(l.target === 0.0F)
    }

    "return 1 for rating = 5" in {
      val l = new Label()
      l.parse(sample(1, 5.0F))
      assert(l.target === 1.0F)
    }
  }

  "Rating" should {
    "parse rating directly" in {
      val r = new Rating()
      r.parse(sample(1, 4.0F))
      assert(r.target === 4.0F)
    }

    "parse rating 1.0" in {
      val r = new Rating()
      r.parse(sample(1, 1.0F))
      assert(r.target === 1.0F)
    }
  }
}
