package pipeline.stats

import com.google.common.io.LittleEndianDataOutputStream
import org.json.JSONObject
import org.scalatest.{Matchers, WordSpec}

import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class AccumulatorBenchmark extends WordSpec with Matchers {

  val N_ADD = 10000000
  val N_MERGE = 1000000
  val WARMUP = 2
  val ITER = 5

  val random = new Random(42)
  val addValues: Array[Float] = Array.fill(N_ADD)(random.nextFloat() * 1000f - 500f)

  def timed(label: String)(block: => Unit): Double = {
    block
    val start = System.nanoTime()
    block
    val elapsed = (System.nanoTime() - start) / 1e9
    println(f"  $label%-30s $elapsed%10.4f s")
    elapsed
  }

  def timedAvg(label: String, warmup: Int, iter: Int, nanos: Boolean = false)(block: => Unit): Double = {
    for (_ <- 1 to warmup) block
    val results = for (_ <- 1 to iter) yield {
      System.gc()
      Thread.sleep(50)
      val start = System.nanoTime()
      block
      (System.nanoTime() - start)
    }
    val avgNs = results.sum.toDouble / iter
    val avgSec = avgNs / 1e9
    val unit = if (nanos) s"${avgNs.toLong} ns" else f"$avgSec%.4f s"
    println(f"  $label%-30s $unit")
    avgSec
  }

  "Accumulator performance benchmark" should {
    "measure add throughput" in {
      println("\n=== Add Throughput ===")

      val sosAdd = timedAvg("SOS add (10^7)", WARMUP, ITER) {
        val s = new SOSAccumulator()
        var i = 0
        while (i < N_ADD) {
          s.add(addValues(i))
          i += 1
        }
      }
      println(f"  SOS: ${N_ADD / sosAdd / 1e6}%.1f M adds/sec")

      val kahanAdd = timedAvg("Kahan SOS add (10^7)", WARMUP, ITER) {
        val k = new KahanSOSAccumulator()
        var i = 0
        while (i < N_ADD) {
          k.add(addValues(i))
          i += 1
        }
      }
      println(f"  Kahan SOS: ${N_ADD / kahanAdd / 1e6}%.1f M adds/sec")
      println(f"  Ratio (Kahan/SOS): ${kahanAdd / sosAdd * 100}%.0f%%")

      val welfAdd = timedAvg("Welford add (10^7)", WARMUP, ITER) {
        val w = new WelfordAccumulator()
        var i = 0
        while (i < N_ADD) {
          w.add(addValues(i))
          i += 1
        }
      }
      println(f"  Welford: ${N_ADD / welfAdd / 1e6}%.1f M adds/sec")
      println(f"  Ratio (Welford/SOS): ${welfAdd / sosAdd * 100}%.0f%%")
      println(f"  SOS overtime: ${(welfAdd / sosAdd - 1.0) * 100}%.1f%%")

      welfAdd should be > 0.0
    }

    "measure merge throughput" in {
      println("\n=== Merge Throughput ===")

      val sosBatches: Array[SOSAccumulator] = Array.tabulate(N_MERGE)(_ => {
        val s = new SOSAccumulator()
        s.add(random.nextFloat() * 100)
        s
      })

      val sosMergeTime = timedAvg("SOS merge (10^6)", WARMUP, ITER) {
        val acc = new SOSAccumulator()
        var i = 0
        while (i < N_MERGE) {
          acc.merge(sosBatches(i))
          i += 1
        }
      }
      println(f"  SOS: ${N_MERGE / sosMergeTime / 1e6}%.1f M merges/sec")

      val welfBatches: Array[WelfordAccumulator] = Array.tabulate(N_MERGE)(_ => {
        val w = new WelfordAccumulator()
        w.add(random.nextFloat() * 100)
        w
      })

      val welfMergeTime = timedAvg("Welford merge (10^6)", WARMUP, ITER) {
        val acc = new WelfordAccumulator()
        var i = 0
        while (i < N_MERGE) {
          acc.merge(welfBatches(i))
          i += 1
        }
      }
      println(f"  Welford: ${N_MERGE / welfMergeTime / 1e6}%.1f M merges/sec")
      println(f"  Ratio (Welford/SOS): ${welfMergeTime / sosMergeTime * 100}%.0f%%")
      println(f"  Welford overhead: ${(welfMergeTime / sosMergeTime - 1.0) * 100}%.1f%%")

      welfMergeTime should be > 0.0
    }

    "measure memory footprint" in {
      println("\n=== Memory Footprint ===")
      println("  Estimated field sizes (JVM 64-bit with compressed OOPs):")
      println("    Object header: 12 bytes")
      println("    Long (count):   8 bytes")
      println("    Double fields:  2 × 8 = 16 bytes")
      println("    Total aligned:  ~32 bytes (plus reference pointer)")

      println("  SOSAccumulator fields:  sum (D), powerSum (D), count (L)")
      println("  WelfordAccumulator fields: count (L), runningMean (D), m2 (D)")
      println("  Both identical: 3 scalars, same memory class")
    }

    "measure serialization size" in {
      println("\n=== Serialization Size ===")

      val pi = PosInfo(
        pos = 42,
        sum = 1234567.890,
        powerSum = 1.23456789e12,
        count = 999999L,
        welfordMean = 1234.5678,
        welfordM2 = 56789.0123
      )

      val jsonObj = new JSONObject()
      jsonObj.put("pos", pi.pos)
      jsonObj.put("sum", pi.sum)
      jsonObj.put("power_sum", pi.powerSum)
      jsonObj.put("count", pi.count)
      jsonObj.put("mean", pi.mean)
      jsonObj.put("std", pi.std)
      if (pi.welfordMean != 0.0 || pi.welfordM2 != 0.0) {
        jsonObj.put("welford_mean", pi.welfordMean)
        jsonObj.put("welford_m2", pi.welfordM2)
      }
      val jsonBytes = jsonObj.toString.getBytes("UTF-8").length
      println(f"  JSON entry (with Welford):  $jsonBytes bytes")

      val jsonObj2 = new JSONObject()
      jsonObj2.put("pos", pi.pos)
      jsonObj2.put("sum", pi.sum)
      jsonObj2.put("power_sum", pi.powerSum)
      jsonObj2.put("count", pi.count)
      jsonObj2.put("mean", pi.mean)
      jsonObj2.put("std", pi.std)
      val jsonWithoutWelford = jsonObj2.toString.getBytes("UTF-8").length
      println(f"  JSON entry (SOS only):     $jsonWithoutWelford bytes")

      val baos = new ByteArrayOutputStream()
      val leWriter = new LittleEndianDataOutputStream(baos)
      leWriter.writeLong(42L)
      leWriter.writeInt(pi.pos)
      leWriter.writeDouble(pi.mean)
      leWriter.writeDouble(pi.std)
      leWriter.close()
      val binBytes = baos.toByteArray.length
      println(f"  Binary entry:              $binBytes bytes")
      println(f"  Binary/JSON ratio:         ${binBytes.toDouble / jsonWithoutWelford * 100}%.0f%%")
    }

    "measure end-to-end pipeline simulation" in {
      println("\n=== End-to-end Pipeline Simulation ===")
      val N_VALUES = 1000000
      val features = 100
      val vals = Array.fill(N_VALUES)(random.nextFloat() * 1000f)

      val sosE2E = timedAvg("SOS map-reduce sim (10^6 vals, 100 keys)", WARMUP, ITER) {
        val maps = ArrayBuffer.fill(features)(new SOSAccumulator())
        var i = 0
        while (i < N_VALUES) {
          val key = i % features
          maps(key).add(vals(i))
          i += 1
        }
        val merged = new SOSAccumulator()
        var k = 0
        while (k < features) {
          merged.merge(maps(k))
          k += 1
        }
      }

      val welfE2E = timedAvg("Welford map-reduce sim (10^6 vals, 100 keys)", WARMUP, ITER) {
        val maps = ArrayBuffer.fill(features)(new WelfordAccumulator())
        var i = 0
        while (i < N_VALUES) {
          val key = i % features
          maps(key).add(vals(i))
          i += 1
        }
        val merged = new WelfordAccumulator()
        var k = 0
        while (k < features) {
          merged.merge(maps(k))
          k += 1
        }
      }
      println(f"  Ratio (Welford/SOS): ${welfE2E / sosE2E * 100}%.0f%%")
    }
  }
}
