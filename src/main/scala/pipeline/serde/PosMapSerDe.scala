package pipeline.serde

import com.google.common.io.{LittleEndianDataInputStream, LittleEndianDataOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json.{JSONArray, JSONObject}

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import utils.LogUtils.green_println
import featurizer.core.FeatureType
import pipeline.stats.PosInfo

/**
 * PosMap serialization — saves/restores feature position maps in JSON/text/binary formats
 */

/** Persists and restores feature position maps, target maps, and field dimension maps in JSON, text, and binary formats. */
class PosMapSerDe(val hadoopConf: Configuration) {
  /** Reconstructs a PosInfo from legacy mean/std representation (used when restoring older binary format). */
  def legacyPosInfo(pos: Int, mean: Double, std: Double, count: Long): PosInfo = {
    val safeCount = math.max(count, 1L)
    val variance = math.max(std * std - 0.000001D, 0.0D)
    val sum = mean * safeCount.toDouble
    val powerSum = (variance + mean * mean) * safeCount.toDouble
    PosInfo(pos, sum, powerSum, safeCount)
  }

  /** Reads all lines from a BufferedReader into a single string. */
  def readText(reader: BufferedReader): String = {
    val content = new StringBuilder()
    var line = reader.readLine()
    var firstLine = true
    while (line != null) {
      if (!firstLine) {
        content.append('\n')
      }
      content.append(line)
      firstLine = false
      line = reader.readLine()
    }
    content.toString()
  }

  /** Restores pos-map, target-map, and dim-map from a JSON file. Returns false if file does not exist. */
  def restoreFromJson(path: String,
                      yesterday: String,
                      posMap: mutable.HashMap[(Int, Long), PosInfo],
                      targetMap: mutable.HashMap[Int, Int],
                      posDimMap: mutable.HashMap[(String, Int, Int), Int]): Boolean = {
    val jsonPath = s"${path}/${yesterday}/pos_map.json"
    val fs = FileSystem.get(URI.create(jsonPath), hadoopConf)
    val file = new Path(jsonPath)
    if (!fs.exists(file)) {
      return false
    }

    val reader = new BufferedReader(new InputStreamReader(fs.open(file), "utf-8"))
    try {
      val root = new JSONObject(readText(reader))
      val targets = root.optJSONObject("targets")
      if (targets != null) {
        val keys = targets.keys()
        while (keys.hasNext) {
          val rawTarget = keys.next()
          targetMap.put(rawTarget.toInt, targets.getInt(rawTarget))
        }
      }

      val features = root.optJSONArray("features")
      var featureIndex = 0
      while (features != null && featureIndex < features.length()) {
        val feature = features.getJSONObject(featureIndex)
        val f_name = feature.getString("field_name")
        val f_index = feature.getInt("field_index")
        val f_type = feature.getInt("field_type")
        posDimMap.put((f_name, f_index, f_type), feature.getInt("dim"))

        val entries = feature.optJSONArray("entries")
        var entryIndex = 0
        while (entries != null && entryIndex < entries.length()) {
          val entry = entries.getJSONObject(entryIndex)
          val posInfo = PosInfo(
            pos = entry.getInt("pos"),
            sum = entry.getDouble("sum"),
            powerSum = entry.getDouble("power_sum"),
            count = entry.getLong("count")
          )
          posMap.put((f_index, entry.getLong("hash")), posInfo)
          entryIndex = entryIndex + 1
        }
        featureIndex = featureIndex + 1
      }
    } finally {
      reader.close()
    }
    true
  }

  /** Restores the dimension-map from a CSV text file. Returns false if file does not exist. */
  def restoreFromText(path: String,
                      yesterday: String,
                      posDimMap: mutable.HashMap[(String, Int, Int), Int]): Boolean = {
    val textPath = s"${path}/${yesterday}/pos_map.txt"
    val fs = FileSystem.get(URI.create(textPath), hadoopConf)
    val file = new Path(textPath)
    if (!fs.exists(file)) {
      return false
    }

    val reader = new BufferedReader(new InputStreamReader(fs.open(file), "utf-8"))
    try {
      var line = reader.readLine()
      line = reader.readLine()
      while (line != null) {
        val items = line.split(",")
        if (items.length >= 4) {
          posDimMap.put((items(0), items(1).toInt, items(2).toInt), items(3).toInt)
        }
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    true
  }

  /** Restores pos-map, target-map, and dim-map from the legacy binary format. */
  def restoreFromBin(path: String,
                     yesterday: String,
                     posMap: mutable.HashMap[(Int, Long), PosInfo],
                     targetMap: mutable.HashMap[Int, Int],
                     posDimMap: mutable.HashMap[(String, Int, Int), Int]): Unit = {
    val binPath = s"${path}/${yesterday}/pos_map.bin"
    try {
      val fs = FileSystem.get(URI.create(binPath), hadoopConf)
      val reader = new LittleEndianDataInputStream(new BufferedInputStream(fs.open(new Path(binPath))))
      try {
        val timestamp = reader.readLong()
        var size = reader.readInt()
        green_println(s"timestamp: ${timestamp}")
        green_println(s"pos_map size: ${size}")

        while (size > 0) {
          val f_name = reader.readUTF()
          val f_index = reader.readInt()
          val f_type = reader.readInt()
          val dim = reader.readInt()
          val hash = reader.readLong()
          val pos = reader.readInt()
          val mean = reader.readDouble()
          val std = reader.readDouble()
          posMap.put((f_index, hash), legacyPosInfo(pos, mean, std, 1L))
          posDimMap.put((f_name, f_index, f_type), dim)
          size = size - 1
        }

        size = reader.readInt()
        green_println(s"target_map size = ${size}")
        while (size > 0) {
          val target_id = reader.readInt()
          val pos = reader.readInt()
          if (!targetMap.contains(target_id)) {
            targetMap.put(target_id, pos)
          }
          size = size - 1
        }
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception =>
        green_println(s"restoreFromBin failed for ${binPath}: ${e.toString}")
    }
  }

  /** Restores all maps (pos, target, dim) from JSON (primary). Falls back gracefully if files are missing. */
  def restore(path: String, yesterday: String): (mutable.HashMap[(Int, Long), PosInfo], mutable.HashMap[Int, Int], mutable.HashMap[(String, Int, Int), Int]) = {
    val pos_dim_map = new mutable.HashMap[(String, Int, Int), Int]()
    val pos_map = new mutable.HashMap[(Int, Long), PosInfo]()
    val target_map = new mutable.HashMap[Int, Int]()

    restoreFromJson(path, yesterday, pos_map, target_map, pos_dim_map)

    green_println(s"read pos_map size = ${pos_map.size}")
    green_println(s"read target_map size = ${target_map.size}")
    green_println(s"read pos_dim_map size = ${pos_dim_map.size}")
    (pos_map, target_map, pos_dim_map)
  }

  /** Saves all maps in all three formats (JSON, text, binary). */
  def save(path: String,
           yesterday: String,
           pos_map: collection.Map[(Int, Long), PosInfo],
           target_map: collection.Map[Int, Int],
           pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    saveToJson(path, yesterday, pos_map, target_map, pos_dim)
    saveToText(path, yesterday, pos_dim)
    saveToBin(path, yesterday, pos_map, target_map, pos_dim)
    green_println(s"write pos_map size: ${pos_map.size}")
    green_println(s"write target_map size: ${target_map.size}")
    green_println(s"write pos_dim size: ${pos_dim.size}")
  }

  /** Saves pos-map, target-map, and dim-map in compact binary format (used for online inference). */
  def saveToBin(path: String,
                yesterday: String,
                pos_map: collection.Map[(Int, Long), PosInfo],
                target_map: collection.Map[Int, Int],
                pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    val binPath = s"${path}/${yesterday}/pos_map.bin"
    green_println(s"write pos_map.bin path = ${binPath}")
    val fs = FileSystem.get(URI.create(binPath), hadoopConf)
    val writer = new LittleEndianDataOutputStream(new BufferedOutputStream(fs.create(new Path(binPath), true)))

    val pos_dim_map = new mutable.HashMap[Int, (String, Int, Int)]()
    val iter = pos_dim.iterator
    while (iter.hasNext) {
      val e = iter.next()
      pos_dim_map.put(e._1._2, (e._1._1, e._1._3, e._2))
    }

    writer.writeLong(yesterday.replaceAll("-", "").toLong)
    writer.writeInt(pos_map.size)

    val iterator = pos_map.iterator
    while (iterator.hasNext) {
      val kv = iterator.next()
      val (f_name, f_type, dim) = pos_dim_map(kv._1._1)
      val (mean, std) = {
        if (f_type == FeatureType.Categorical) {
          (0.0D, 1.0D)
        } else {
          (kv._2.mean, kv._2.std)
        }
      }

      writer.writeUTF(f_name)
      writer.writeInt(kv._1._1)
      writer.writeInt(f_type)
      writer.writeInt(dim)
      writer.writeLong(kv._1._2)
      writer.writeInt(kv._2.pos)
      writer.writeDouble(mean)
      writer.writeDouble(std)
    }

    writer.writeInt(target_map.size)
    val it = target_map.iterator
    while (it.hasNext) {
      val kv = it.next()
      writer.writeInt(kv._1)
      writer.writeInt(kv._2)
    }
    writer.close()
  }

  /** Saves field dimension map as human-readable CSV. */
  def saveToText(path: String,
                 yesterday: String,
                 pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    val textPath = s"${path}/${yesterday}/pos_map.txt"
    green_println(s"write pos_map.text path = ${textPath}")
    val fs = FileSystem.get(URI.create(textPath), hadoopConf)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(textPath), true), "utf-8"))
    try {
      writer.write("field_name,field_index,field_type,dim\n")
      pos_dim.toSeq
        .sortBy { case ((fieldName, fieldIndex, fieldType), _) => (fieldIndex, fieldType, fieldName) }
        .foreach { case ((fieldName, fieldIndex, fieldType), dim) =>
          writer.write(s"${fieldName},${fieldIndex},${fieldType},${dim}\n")
        }
    } finally {
      writer.close()
    }
  }

  /** Saves pos-map, target-map, and dim-map as a structured JSON file (primary readable format). */
  def saveToJson(path: String,
                 yesterday: String,
                 pos_map: collection.Map[(Int, Long), PosInfo],
                 target_map: collection.Map[Int, Int],
                 pos_dim: collection.Map[(String, Int, Int), Int]): Unit = {
    val jsonPath = s"${path}/${yesterday}/pos_map.json"
    green_println(s"write pos_map.json path = ${jsonPath}")
    val fs = FileSystem.get(URI.create(jsonPath), hadoopConf)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(jsonPath), true), "utf-8"))

    try {
      val root = new JSONObject()
      root.put("yesterday", yesterday)

      val features = new JSONArray()
      val pos_dim_map = new mutable.HashMap[Int, (String, Int, Int)]()
      val iter = pos_dim.iterator
      while (iter.hasNext) {
        val e = iter.next()
        pos_dim_map.put(e._1._2, (e._1._1, e._1._3, e._2))
      }

      val pos_map_array = new ArrayBuffer[(Int, Long, PosInfo)]()
      val it = pos_map.iterator
      while (it.hasNext) {
        val e = it.next()
        pos_map_array.append((e._1._1, e._1._2, e._2))
      }

      val iterator = pos_map_array.groupBy(s => s._1).toSeq.sortBy(_._1).iterator
      while (iterator.hasNext) {
        val e = iterator.next()
        val f_index = e._1
        val pos_info = e._2.sortWith((a, b) => a._3.pos < b._3.pos)
        val (f_name, f_type, dim) = pos_dim_map(f_index)

        val feature = new JSONObject()
        feature.put("field_name", f_name)
        feature.put("field_index", f_index)
        feature.put("field_type", f_type)
        feature.put("dim", dim)

        val entries = new JSONArray()
        for ((_, hash, stat) <- pos_info) {
          val entry = new JSONObject()
          val (mean, std) = {
            if (f_type == FeatureType.Categorical) {
              (0.0D, 1.0D)
            } else {
              (stat.mean, stat.std)
            }
          }
          entry.put("hash", hash)
          entry.put("pos", stat.pos)
          entry.put("sum", stat.sum)
          entry.put("power_sum", stat.powerSum)
          entry.put("count", stat.count)
          entry.put("mean", mean)
          entry.put("std", std)
          entries.put(entry)
        }
        feature.put("entries", entries)
        features.put(feature)
      }
      root.put("features", features)
      root.put("feature_size", features.length())
      root.put("target_size", target_map.size)

      val targets = new JSONObject()
      target_map.toSeq.sortBy(_._2)
        .foreach { case (target_id, pos) =>
          targets.put(target_id.toString, pos)
        }
      root.put("targets", targets)

      writer.write(root.toString(2))
      writer.write("\n")
    } finally {
      writer.close()
    }
  }
}
