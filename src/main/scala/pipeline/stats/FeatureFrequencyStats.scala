package pipeline.stats

import org.json.JSONObject

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.collection.mutable

object FeatureFrequencyStats {

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.length >= 2 && args(0) == "--input") args(1) else {
      println("Usage: FeatureFrequencyStats --input <pos_map.json>")
      sys.exit(1)
    }

    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath), "utf-8"))
    val content = try {
      val sb = new StringBuilder()
      var line = reader.readLine()
      while (line != null) {
        sb.append(line).append('\n')
        line = reader.readLine()
      }
      sb.toString()
    } finally {
      reader.close()
    }

    val root = new JSONObject(content)
    val features = root.getJSONArray("features")

    val cnts = mutable.ArrayBuffer.empty[Long]

    for (fi <- 0 until features.length()) {
      val feature = features.getJSONObject(fi)
      val entries = feature.getJSONArray("entries")
      for (ei <- 0 until entries.length()) {
        val entry = entries.getJSONObject(ei)
        cnts.append(entry.getLong("count"))
      }
    }

    val freqMap = mutable.LinkedHashMap.empty[Long, Int]
    for (c <- cnts) {
      freqMap(c) = freqMap.getOrElse(c, 0) + 1
    }
    val sorted = freqMap.toSeq.sortBy(_._1)

    println(f"\nFeature frequency distribution (${cnts.length} total entries):")
    println(f"  ${"Count"}%10s  ${"#Features"}%12s  ${"Cumul %"}%10s")
    println("  " + "=" * 36)

    var cumul = 0L
    val total = cnts.length
    for ((cnt, num) <- sorted) {
      cumul += num
      println(f"  $cnt%10d  $num%12d  ${cumul.toDouble / total * 100}%9.1f%%")
    }

    println("  " + "=" * 36)
    println(f"  Total: $total%10d entries")

    val min = cnts.min
    val max = cnts.max
    val mean = cnts.sum.toDouble / cnts.length
    val sortedCnts = cnts.sorted
    val median = sortedCnts(cnts.length / 2)
    println(f"\nSummary: min=$min, max=$max, mean=$mean%.1f, median=$median")

    val once = freqMap.getOrElse(1, 0)
    println(f"Features appearing exactly once: $once (${once.toDouble / total * 100}%.1f%%)")
  }
}
