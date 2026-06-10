package featurizer.core

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}
import utils.MurmurHash3
import utils.MurmurHash3.LongPair

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** This cross featurizer encodes feature crosses (combinations of multiple categorical features) into embedding index. */
class CrossFeature[T](f_i: Int, f_n: String, rnfs: CategoricalFeature[T]*) extends RawFeature(f_i, f_n, f_t = FeatureType.Categorical) {
  val indexes: Array[Int] = new Array[Int](rnfs.length)

  val key_len: Int = (4 + 8) * rnfs.length

  def computeHash(dim: Long): Long = {
    if (dim <= 0) return 0L
    val bb = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)
    var shift = 0
    for (i <- 0 until rnfs.length) {
      bb.putInt(shift, rnfs(i).f_index)
      shift += 4
      bb.putLong(shift, rnfs(i).feature_list(indexes(i)))
      shift += 8
    }
    val p: LongPair = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(bb.array(), 0, key_len, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  def formatCombination: String = {
    rnfs.indices.map(
      i => s"${rnfs(i).f_index}:${rnfs(i).raw_list(indexes(i))}"
    ).mkString("__xx__")
  }

  def foreachCombination(body: => Unit): Unit = {
    for (i <- 0 until rnfs.length) {
      indexes(i) = 0
    }
    var done = false
    while (!done) {
      var skip = false
      for (i <- 0 until rnfs.length) {
        if (rnfs(i).feature_list.isEmpty || rnfs(i).feature_list(indexes(i)) == 0) {
          skip = true
        }
      }
      if (!skip) {
        body
      }

      var i = rnfs.length - 1
      var added = false
      while (!added && i >= 0) {
        if (indexes(i) == rnfs(i).feature_list.length - 1) {
          indexes(i) = 0
          i -= 1
        } else {
          indexes(i) += 1
          added = true
        }
      }
      if (!added) {
        done = true
      }
    }
  }

  override def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val buf = ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
    foreachCombination {
      buf.append((f_name, f_index, f_type, formatCombination, computeHash(dim), 1.0F))
    }
    buf
  }

  def get_hash_info(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    get_hash_info(dim)
  }

  override def get_hash(dim: Long): ArrayBuffer[Long] = {
    val buf = new ArrayBuffer[Long]
    foreachCombination {
      buf.append(computeHash(dim))
    }
    buf
  }

  def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    foreachCombination {
      raw_buf.append(formatCombination)
      pos_buf.append(computeHash(dim))
      value_buf.append(1.0F)
      has_feature = true
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  def add(input: T, dim: Long, builder: Example.Builder): Unit = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    add(dim, builder)
  }

  def add(dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    foreachCombination {
      val fmt = formatCombination
      val hash = computeHash(dim)
      if (pos_map.contains((f_index, hash))) {
        val pos = pos_map((f_index, hash))
        raw_buf.append(fmt)
        pos_buf.append(pos)
        value_buf.append(1.0F)
        has_feature = true
      }
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  def add(input: T, dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    add(dim, builder, pos_map)
  }

  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    foreachCombination {
      encoded_map.getOrElseUpdate(f_name, ArrayBuffer.empty[Long]).append(computeHash(dim))
    }
  }

  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    var has_feature = false
    foreachCombination {
      pos_map.get((f_index, computeHash(dim))).foreach { pos =>
        encoded_map.getOrElseUpdate(f_name, ArrayBuffer.empty[Long]).append(pos)
        has_feature = true
      }
    }
    has_feature
  }

  def add(dim: Long, pos_map: collection.Map[(Int, Long), Int], columns: mutable.Map[String, Any]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)
    foreachCombination {
      val fmt = formatCombination
      val hash = computeHash(dim)
      if (pos_map.contains((f_index, hash))) {
        raw_buf.append(fmt)
        pos_buf.append(pos_map((f_index, hash)).toLong)
        value_buf.append(1.0F)
      }
    }
    columns.put(f_name + "_raw", raw_buf)
    columns.put(f_name + "_index", pos_buf)
    columns.put(f_name + "_value", value_buf)
    pos_buf.length > 1
  }
}
