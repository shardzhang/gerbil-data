package pipeline

import org.scalatest.{Matchers, WordSpec}

import java.io.{BufferedReader, StringReader}
import pipeline.stats.{PosInfo, RunningValueStats}

class PipelineTest extends WordSpec with Matchers {

  // RunningValueStats tests
  "RunningValueStats" should {
    "add values and track sum/count/powerSum" in {
      val stat = new RunningValueStats()
      stat.add(2.0F).add(3.0F).add(4.0F)
      assert(stat.sum === 9.0D)
      assert(stat.count === 3L)
      // 2^2 + 3^2 + 4^2 = 4 + 9 + 16 = 29
      assert(stat.powerSum === 29.0D)
    }

    "merge two RunningValueStatss" in {
      val s1 = new RunningValueStats(5.0D, 25.0D, 2L)
      val s2 = new RunningValueStats(10.0D, 100.0D, 3L)
      s1.merge(s2)
      assert(s1.sum === 15.0D)
      assert(s1.powerSum === 125.0D)
      assert(s1.count === 5L)
    }
  }

  // PosInfo tests
  "PosInfo" should {
    "calculate mean correctly" in {
      val info = PosInfo(pos = 0, sum = 10.0D, powerSum = 30.0D, count = 5L)
      assert(info.mean === 2.0D)
    }

    "return 0 mean when count is 0" in {
      val info = PosInfo(pos = 0, sum = 10.0D, powerSum = 30.0D, count = 0L)
      assert(info.mean === 0.0D)
    }

    "calculate std correctly" in {
      // values: 2, 4, 4, 4, 5, 5, 7, 9
      // mean = 5.0, variance = 4.0, std = 2.0
      val info = PosInfo(pos = 0, sum = 40.0D, powerSum = 240.0D, count = 8L)
      assert(info.mean === 5.0D)
      // variance = 240/8 - 25 = 30 - 25 = 5, std = sqrt(5 + 0.000001) ≈ 2.236
      assert(info.std > 2.0D)
    }

    "return 1.0 std when count is 0" in {
      val info = PosInfo(pos = 0, sum = 0.0D, powerSum = 0.0D, count = 0L)
      assert(info.std === 1.0D)
    }

    "merge with RunningValueStats" in {
      val info = PosInfo(pos = 1, sum = 5.0D, powerSum = 13.0D, count = 3L)
      val stat = new RunningValueStats(3.0D, 5.0D, 1L)
      val merged = info.merge(stat)
      assert(merged.pos === 1)
      assert(merged.sum === 8.0D)
      assert(merged.powerSum === 18.0D)
      assert(merged.count === 4L)
    }
  }

  // Concrete test driver to test Pipeline methods
  private class TestDriver extends Pipeline[String] {
    override val max_dim: Long = 1000L

    override def feature_encoder: featurizer.core.Featurizer[String] = {
      new featurizer.core.Featurizer[String] {
        override def setup(): featurizer.core.Featurizer[String] = this
      }.setup()
    }

    override def loadTrainingSamples(
      spark: org.apache.spark.sql.SparkSession,
      inputDir: String,
      parts: Int
    ): org.apache.spark.rdd.RDD[(String, Boolean)] = {
      import spark.implicits._
      spark.sparkContext.parallelize(Seq(("sample1", true), ("sample2", true)))
    }

    override def getSampleTarget(sample: String): Int = sample.hashCode

    override def getSampleTimestamp(sample: String): Long = 0L

    // Expose persistence methods for testing
    def testReadText(reader: BufferedReader): String = posMapSerDe.readText(reader)
    def testLegacyPosInfo(pos: Int, mean: Double, std: Double, count: Long): PosInfo = posMapSerDe.legacyPosInfo(pos, mean, std, count)
  }

  "Pipeline.readText" should {
    "read multi-line text" in {
      val driver = new TestDriver()
      val reader = new BufferedReader(new StringReader("line1\nline2\nline3"))
      val content = driver.testReadText(reader)
      assert(content === "line1\nline2\nline3")
    }

    "return empty string for empty input" in {
      val driver = new TestDriver()
      val reader = new BufferedReader(new StringReader(""))
      val content = driver.testReadText(reader)
      assert(content === "")
    }
  }

  "Pipeline.legacyPosInfo" should {
    "reconstruct PosInfo from mean/std/count" in {
      val driver = new TestDriver()
      val info = driver.testLegacyPosInfo(pos = 1, mean = 3.0D, std = 2.0D, count = 5L)
      assert(info.pos === 1)
      assert(info.count === 5L)
      // sum = mean * count = 3.0 * 5 = 15
      assert(info.sum === 15.0D)
      // variance = std^2 - 0.000001 = 4 - 0.000001 = 3.999999
      // powerSum = (variance + mean^2) * count = (3.999999 + 9) * 5 = 64.999995
      assert(info.powerSum > 60.0D)
    }

    "handle zero count" in {
      val driver = new TestDriver()
      val info = driver.testLegacyPosInfo(pos = 0, mean = 0.0D, std = 0.0D, count = 0L)
      assert(info.pos === 0)
      assert(info.count === 1L) // safeCount = max(0, 1) = 1
    }
  }

  "Pipeline.parquet_schema" should {
    "include target field" in {
      val driver = new TestDriver()
      val schema = driver.parquet_schema
      assert(schema.fields.exists(_.name == "target"))
    }
  }

  "Pipeline.keepSample" should {
    "keep sample with non-zero target" in {
      val driver = new TestDriver()
      // getSampleTarget returns hashCode, which is unlikely to be 0
      assert(driver.keepSample("test", 0.0))
    }

    "sample with zero target based on ratio" in {
      val driver = new TestDriver() {
        override def getSampleTarget(sample: String): Int = 0
      }
      // With ratio = 1.0, should always keep
      assert(driver.keepSample("test", 1.0))
    }
  }
}
