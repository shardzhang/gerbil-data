package pipeline.stats

import org.scalatest.{Matchers, WordSpec}

class ValueStatsTest extends WordSpec with Matchers {

  "SOSAccumulator" should {
    "start with zero values" in {
      val s = new SOSAccumulator()
      assert(s.sum === 0.0)
      assert(s.powerSum === 0.0)
      assert(s.count === 0L)
    }

    "accumulate single value correctly" in {
      val s = new SOSAccumulator()
      s.add(3.0F)
      assert(s.sum === 3.0)
      assert(s.powerSum === 9.0)
      assert(s.count === 1L)
    }

    "accumulate multiple values" in {
      val s = new SOSAccumulator()
      s.add(1.0F)
      s.add(2.0F)
      s.add(3.0F)
      assert(s.sum === 6.0)
      assert(s.powerSum === 14.0)
      assert(s.count === 3L)
    }

    "merge two stats correctly" in {
      val a = new SOSAccumulator()
      a.add(1.0F)
      a.add(2.0F)

      val b = new SOSAccumulator()
      b.add(3.0F)
      b.add(4.0F)

      a.merge(b)
      assert(a.sum === 10.0)
      assert(a.powerSum === 30.0)
      assert(a.count === 4L)
    }

    "be chainable" in {
      val s = new SOSAccumulator()
      s.add(5.0F).add(5.0F)
      assert(s.sum === 10.0)
      assert(s.count === 2L)
    }

    "handle large values without overflow" in {
      val s = new SOSAccumulator()
      s.add(1e10F)
      s.add(-1e10F)
      assert(s.sum === 0.0)
      assert(s.count === 2L)
    }

    "compute mean correctly" in {
      val s = new SOSAccumulator()
      s.add(2.0F)
      s.add(4.0F)
      assert(s.mean === 3.0)
    }

    "compute std correctly" in {
      val s = new SOSAccumulator()
      s.add(1.0F)
      s.add(2.0F)
      s.add(3.0F)
      val variance = 2.0 / 3.0
      val expectedStd = math.sqrt(variance + 0.000001)
      assert(math.abs(s.std - expectedStd) < 1e-12)
    }
  }

  "WelfordAccumulator" should {
    "start with zero values" in {
      val w = new WelfordAccumulator()
      assert(w.count === 0L)
      assert(w.runningMean === 0.0)
      assert(w.m2 === 0.0)
    }

    "accumulate single value correctly" in {
      val w = new WelfordAccumulator()
      w.add(3.0F)
      assert(w.count === 1L)
      assert(w.runningMean === 3.0)
      assert(w.m2 === 0.0)
      assert(w.mean === 3.0)
    }

    "accumulate multiple values correctly" in {
      val w = new WelfordAccumulator()
      w.add(1.0F)
      w.add(2.0F)
      w.add(3.0F)
      assert(w.count === 3L)
      assert(w.runningMean === 2.0)
      assert(math.abs(w.m2 - 2.0) < 1e-12)
      assert(w.mean === 2.0)
      val expectedStd = math.sqrt(2.0 / 3.0 + 0.000001)
      assert(math.abs(w.std - expectedStd) < 1e-12)
    }

    "merge two accumulators correctly (Chan's formula)" in {
      val a = new WelfordAccumulator()
      a.add(1.0F)
      a.add(3.0F)
      a.add(5.0F)

      val b = new WelfordAccumulator()
      b.add(7.0F)
      b.add(9.0F)

      a.merge(b)
      assert(a.count === 5L)
      assert(math.abs(a.runningMean - 5.0) < 1e-10)
      assert(math.abs(a.m2 - 40.0) < 1e-10)
      assert(math.abs(a.mean - 5.0) < 1e-12)
    }

    "be chainable" in {
      val w = new WelfordAccumulator()
      w.add(1.0F).add(2.0F).add(3.0F)
      assert(w.count === 3L)
      assert(w.runningMean === 2.0)
    }

    "produce same mean and std as SOS for normal data" in {
      val sos = new SOSAccumulator()
      val welf = new WelfordAccumulator()
      val values = Array(1.5F, 2.3F, 4.7F, 3.1F, 2.8F, 5.0F)
      for (v <- values) {
        sos.add(v)
        welf.add(v)
      }
      assert(math.abs(sos.mean - welf.mean) < 1e-12)
      assert(math.abs(sos.std - welf.std) < 1e-12)
    }

    "return 0.0 mean when count is 0" in {
      val w = new WelfordAccumulator()
      assert(w.mean === 0.0)
    }

    "return 1.0 std when count is 0" in {
      val w = new WelfordAccumulator()
      assert(w.std === 1.0)
    }

    "maintain numerical stability for high-mean-low-variance data" in {
      val w = new WelfordAccumulator()
      val v = 1000000.1F
      val expectedMean = v.toDouble
      for (_ <- 1 to 100000) {
        w.add(v)
      }
      assert(math.abs(w.runningMean - expectedMean) < 1e-9)
      assert(math.abs(w.m2) < 1e-9)
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
      assert(p.mean === 2.0)
    }

    "compute std correctly" in {
      val p = PosInfo(pos = 1, sum = 6.0, powerSum = 14.0, count = 3L)
      val variance = 14.0 / 3.0 - math.pow(2.0, 2)
      val expectedStd = math.sqrt(variance + 0.000001)
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

    "merge with SOSAccumulator" in {
      val p = PosInfo(pos = 3, sum = 10.0, powerSum = 30.0, count = 5L)
      val stats = new SOSAccumulator()
      stats.add(5.0F)
      stats.add(5.0F)

      val merged = p.merge(stats)
      assert(merged.pos === 3)
      assert(merged.sum === 20.0)
      assert(merged.powerSum === 80.0)
      assert(merged.count === 7L)
    }

    "not mutate original PosInfo on merge" in {
      val p = PosInfo(pos = 1, sum = 5.0, powerSum = 25.0, count = 2L)
      val stats = new SOSAccumulator()
      stats.add(1.0F)

      p.merge(stats)
      assert(p.sum === 5.0)
      assert(p.count === 2L)
    }

    "merge with WelfordAccumulator" in {
      val p = PosInfo(pos = 3, sum = 0.0, powerSum = 0.0, count = 0L)
      val w = new WelfordAccumulator()
      w.add(2.0F)
      w.add(4.0F)
      w.add(6.0F)

      val merged = p.merge(w)
      assert(merged.pos === 3)
      assert(merged.count === 3L)
      assert(math.abs(merged.mean - 4.0) < 1e-12)
      assert(math.abs(merged.sum - 12.0) < 1e-12)
      assert(math.abs(merged.powerSum - 56.0) < 1e-12)
      assert(math.abs(merged.welfordMean - 4.0) < 1e-12)
      assert(math.abs(merged.welfordM2 - 8.0) < 1e-12)
    }

    "merge Welford into SOS-constructed PosInfo correctly" in {
      val p = PosInfo(pos = 1, sum = 12.0, powerSum = 56.0, count = 3L)
      val w = new WelfordAccumulator()
      w.add(8.0F)
      w.add(10.0F)

      val merged = p.merge(w)
      assert(merged.count === 5L)
      assert(math.abs(merged.mean - 6.0) < 1e-10)
      val expectedStd = math.sqrt(40.0 / 5.0 + 0.000001)
      assert(math.abs(merged.std - expectedStd) < 1e-10)
    }

    "fromSOS factory constructs with SOS fields" in {
      val p = PosInfo.fromSOS(pos = 2, sum = 10.0, powerSum = 30.0, count = 5L)
      assert(p.pos === 2)
      assert(p.sum === 10.0)
      assert(p.powerSum === 30.0)
      assert(p.count === 5L)
      assert(p.welfordMean === 0.0)
      assert(p.welfordM2 === 0.0)
    }

    "fromWelford factory constructs with both SOS and Welford fields" in {
      val p = PosInfo.fromWelford(pos = 2, runningMean = 4.0, m2 = 8.0, n = 3L)
      assert(p.pos === 2)
      assert(p.count === 3L)
      assert(math.abs(p.welfordMean - 4.0) < 1e-12)
      assert(math.abs(p.welfordM2 - 8.0) < 1e-12)
      assert(math.abs(p.sum - 12.0) < 1e-12)
      assert(math.abs(p.powerSum - 56.0) < 1e-12)
    }

    "fromWelford handles zero count" in {
      val p = PosInfo.fromWelford(pos = 0, runningMean = 0.0, m2 = 0.0, n = 0L)
      assert(p.count === 0L)
      assert(p.sum === 0.0)
      assert(p.powerSum === 0.0)
    }
  }
}
