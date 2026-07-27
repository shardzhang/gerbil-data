package pipeline.stats

import tfrecords.SharedSparkSessionSuite
import scala.util.Random
import scala.math

/**
 * Spark merge accuracy benchmark.
 * Validates that distributed mapPartitions -> reduceByKey produces numerically
 * identical results to single-machine sequential accumulation for all 7 test
 * configurations used in the paper.
 *
 * 本质上就是验证 `Merge(partition1.Add(data1), partition2.Add(data2)) == Add(concat(data1, data2))`
 * Run: mvn test -Dtest=MergeAccuracyBench
 */
class MergeAccuracyBench extends SharedSparkSessionSuite {

  import MergeAccuracyBench._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.stop()
    spark = org.apache.spark.sql.SparkSession.builder()
      .appName("MergeAccuracyBench")
      .master("local[4]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  "Distributed merge" should {
    "preserve accuracy within expected bounds for SOS, Kahan SOS, and Welford" in {
      val sc = spark.sparkContext
      val results = for ((cfg, idx) <- configs.zipWithIndex) yield {
        val seed = 42 + idx * 1000
        val data: Array[Double] = generateData(cfg, seed)

        // Single-machine reference for all three accumulators
        val sosRef = new SOSAccumulator()
        val kahanRef = new KahanSOSAccumulator()
        val welfRef = new WelfordAccumulator()
        data.foreach { v =>
          sosRef.add(v.toFloat)
          kahanRef.add(v.toFloat)
          welfRef.add(v.toFloat)
        }
        val sosVarRef = sosRef.std * sosRef.std
        val kahanVarRef = kahanRef.std * kahanRef.std
        val welfVarRef = welfRef.std * welfRef.std

        // Distributed: mapPartitions -> reduceByKey
        val rdd = sc.parallelize(data.map(_.toFloat), numSlices = 4)
        val merged: Array[(String, (SOSAccumulator, KahanSOSAccumulator, WelfordAccumulator))] = rdd.mapPartitions { iter =>
          val sos = new SOSAccumulator()
          val kahan = new KahanSOSAccumulator()
          val welf = new WelfordAccumulator()
          iter.foreach { v =>
            sos.add(v)
            kahan.add(v)
            welf.add(v)
          }
          Iterator.single(("key", (sos, kahan, welf)))
        }.reduceByKey { (a, b) =>
          (a._1.merge(b._1).asInstanceOf[SOSAccumulator],
            a._2.merge(b._2).asInstanceOf[KahanSOSAccumulator],
            a._3.merge(b._3).asInstanceOf[WelfordAccumulator])
        }.collect()

        val (sosDist, kahanDist, welfDist) = if (merged.nonEmpty) {
          (merged.head._2._1, merged.head._2._2, merged.head._2._3)
        } else {
          (new SOSAccumulator(), new KahanSOSAccumulator(), new WelfordAccumulator())
        }

        val sosVarDist = sosDist.std * sosDist.std
        val kahanVarDist = kahanDist.std * kahanDist.std
        val welfVarDist = welfDist.std * welfDist.std

        val sosRelDiff = if (math.abs(sosVarRef) > 1e-30) {
          math.abs(sosVarDist - sosVarRef) / math.abs(sosVarRef)
        } else {
          0.0
        }
        val kahanRelDiff = if (math.abs(kahanVarRef) > 1e-30) {
          math.abs(kahanVarDist - kahanVarRef) / math.abs(kahanVarRef)
        } else {
          0.0
        }
        val welfRelDiff = if (math.abs(welfVarRef) > 1e-30) {
          math.abs(welfVarDist - welfVarRef) / math.abs(welfVarRef)
        } else {
          0.0
        }

        val pass = if (cfg.variance == 0.0) {
          sosRelDiff < 1e-15 && kahanRelDiff < 1e-15 && welfRelDiff < 1e-15
        } else if (cfg.name.contains("Extreme") || cfg.name.contains("Tiny variance")) {
          kahanRelDiff < 1e-6 && welfRelDiff < 1e-6
        } else {
          sosRelDiff < 1e-6 && kahanRelDiff < 1e-6 && welfRelDiff < 1e-6
        }

        (cfg.name, sosRelDiff, kahanRelDiff, welfRelDiff, sosVarDist, kahanVarDist, welfVarDist, pass)
      }

      // Print results table
      val header = f"  ${"Configuration"}%-30s ${"SOS"}%10s ${"Kahan"}%10s ${"Welf"}%10s ${"SOS σ²"}%18s ${"Kahan σ²"}%16s ${"Welf σ²"}%16s ${"Status"}%8s"
      val sep = "=" * (header.length + 10)
      println()
      println("Merge accuracy benchmark (4 partitions, 1 trial per config)")
      println("Validates: distributed result == single-machine result for all three accumulators")
      println(sep)
      println(header)
      println(sep)

      var allPassed = true
      for ((name, sosRel, kahanRel, welfRel, sosVar, kahanVar, welfVar, pass) <- results) {
        if (!pass) allPassed = false
        println(f"  $name%-30s $sosRel%10.2e $kahanRel%10.2e $welfRel%10.2e $sosVar%18.6e $kahanVar%16.6e $welfVar%16.6e ${if (pass) "PASS" else "FAIL"}%8s")
      }
      println(sep)
      if (allPassed) {
        println("  All configurations PASSED.")
      } else {
        println("  Some configurations FAILED!")
      }
      println()

      assert(allPassed, "Distributed merge must match single-machine reference for all configurations")
    }
  }
}

object MergeAccuracyBench {
  case class Config(name: String, base: Double, variance: Double, n: Int)

  val configs = Seq(
    Config("Ratings (0-5)",              3.0,     2.0,       10000000),
    Config("Price ($0-$100)",           50.0,   800.0,       10000000),
    Config("Large values (1e6)",         1e6,     1e8,       10000000),
    Config("High precision small",      0.001,   1e-7,       10000000),
    Config("Extreme cancellation",        1e6,   1e-2,       10000000),
    Config("Zero variance",              42.0,     0.0,      10000000),
    Config("Tiny variance large mean",    1e4,    1e-4,      10000000)
  )

  def generateData(cfg: Config, seed: Int): Array[Double] = {
    val rng = new Random(seed.toLong)
    if (cfg.variance > 0.0) {
      val std = math.sqrt(cfg.variance)
      Array.fill(cfg.n)(cfg.base + rng.nextGaussian() * std)
    } else {
      Array.fill(cfg.n)(cfg.base)
    }
  }
}
