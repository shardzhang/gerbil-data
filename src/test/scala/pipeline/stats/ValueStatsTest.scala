package pipeline.stats

import org.scalatest.{Matchers, WordSpec}

class ValueStatsTest extends WordSpec with Matchers {

  "RunningValueStats" should {
    "start with zero values" in {
      val s = new RunningValueStats()
      assert(s.sum === 0.0)
      assert(s.powerSum === 0.0)
      assert(s.count === 0L)
    }

    "accumulate single value correctly" in {
      val s = new RunningValueStats()
      s.add(3.0F)
      assert(s.sum === 3.0)
      assert(s.powerSum === 9.0)
      assert(s.count === 1L)
    }

    "accumulate multiple values" in {
      val s = new RunningValueStats()
      s.add(1.0F)
      s.add(2.0F)
      s.add(3.0F)
      assert(s.sum === 6.0)
      assert(s.powerSum === 14.0) // 1 + 4 + 9
      assert(s.count === 3L)
    }

    "merge two stats correctly" in {
      val a = new RunningValueStats()
      a.add(1.0F)
      a.add(2.0F)

      val b = new RunningValueStats()
      b.add(3.0F)
      b.add(4.0F)

      a.merge(b)
      assert(a.sum === 10.0)
      assert(a.powerSum === 30.0) // 1 + 4 + 9 + 16
      assert(a.count === 4L)
    }

    "be chainable" in {
      val s = new RunningValueStats()
      s.add(5.0F).add(5.0F)
      assert(s.sum === 10.0)
      assert(s.count === 2L)
    }

    "handle large values without overflow" in {
      val s = new RunningValueStats()
      s.add(1e10F)
      s.add(-1e10F)
      assert(s.sum === 0.0)
      assert(s.count === 2L)
    }
  }

  "PosInfo" should {
    "create with position only" in {
      val p = PosInfo(pos = 5)
      assert(p.pos === 5)
      assert(p.sum === 0.0)
      assert(p.powerSum === 0.0)
      assert(p.count === 0L)
    }

    "compute mean correctly" in {
      val p = PosInfo(pos = 1, sum = 10.0, powerSum = 30.0, count = 5L)
      assert(p.mean === 2.0) // 10 / 5
    }

    "compute std correctly" in {
      // values = [1, 2, 3], mean = 2, variance = (1+0+1)/3 = 0.667, std = 0.816
      val p = PosInfo(pos = 1, sum = 6.0, powerSum = 14.0, count = 3L)
      val expectedStd = math.sqrt(14.0 / 3 - math.pow(2.0, 2) + 0.000001)
      assert(p.std === expectedStd)
    }

    "return 0 mean for zero count" in {
      val p = PosInfo(pos = 1)
      assert(p.mean === 0.0)
    }

    "return 1 std for zero count" in {
      val p = PosInfo(pos = 1)
      assert(p.std === 1.0)
    }

    "merge with RunningValueStats" in {
      val p = PosInfo(pos = 3, sum = 10.0, powerSum = 30.0, count = 5L)
      val stats = new RunningValueStats()
      stats.add(5.0F)
      stats.add(5.0F)

      val merged = p.merge(stats)
      assert(merged.pos === 3)
      assert(merged.sum === 20.0) // 10 + 5 + 5
      assert(merged.powerSum === 80.0) // 30 + 25 + 25
      assert(merged.count === 7L) // 5 + 2
    }

    "not mutate original PosInfo on merge" in {
      val p = PosInfo(pos = 1, sum = 5.0, powerSum = 25.0, count = 2L)
      val stats = new RunningValueStats()
      stats.add(1.0F)

      p.merge(stats)
      assert(p.sum === 5.0) // unchanged
      assert(p.count === 2L) // unchanged
    }
  }
}
