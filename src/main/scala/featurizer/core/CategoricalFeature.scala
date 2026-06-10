package featurizer.core

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}
import utils.MurmurHash3
import utils.MurmurHash3.LongPair

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** This categorical featurizer encodes discrete id into embedding index. */
abstract class CategoricalFeature[T](f_i: Int, f_n: String, f_t: Byte = FeatureType.Categorical) extends RawFeature(f_i, f_n, f_t) {

  def parse(sample: T): RawFeature

  var raw_list: ArrayBuffer[String] = new ArrayBuffer[String]()
  var feature_list: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  var value_list: ArrayBuffer[Float] = new ArrayBuffer[Float]()

  val key_len: Int = 4 + 8

  def clear(): Unit = {
    raw_list.clear()
    feature_list.clear()
    value_list.clear()
  }

  def computeHash(fea: Long, dim: Long): Long = {
    if (dim <= 0) return fea % math.max(dim, 1L)
    val bb = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(0, f_index)
    bb.putLong(4, fea)
    val p: LongPair = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(bb.array(), 0, key_len, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  override def get_hash(dim: Long): ArrayBuffer[Long] = {
    val pos_list = new ArrayBuffer[Long]()
    for (i <- feature_list.indices) {
      val fea = feature_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        pos_list.append(hash)
      }
    }
    pos_list
  }

  override def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val pos_info_list = new ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
    for (i <- feature_list.indices) {
      val fea = feature_list(i)
      val value = value_list(i)
      val raw_fea = raw_list(i)
      if (fea != 0) {
        val fmt = f_index.toString + ":" + raw_fea
        val hash = computeHash(fea, dim)
        pos_info_list.append((f_name, f_index, f_type, fmt, hash, value))
      }
    }
    pos_info_list
  }

  def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    if (raw_list.size != feature_list.size || raw_list.size != value_list.size) {
      throw new IllegalArgumentException("raw_buf, pos_buf, value_buf size not equal")
    }
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)
    for (i <- feature_list.indices) {
      val raw_fea = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        raw_buf.append(raw_fea)
        pos_buf.append(hash)
        value_buf.append(value)
      }
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
  }

  def add(dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    if (raw_list.size != feature_list.size || raw_list.size != value_list.size) {
      throw new IllegalArgumentException("raw_buf, pos_buf, value_buf size not equal")
    }
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    for (i <- feature_list.indices) {
      val raw_fea = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (pos_map.contains((f_index, hash))) {
          raw_buf.append(raw_fea)
          val pos = pos_map((f_index, hash))
          pos_buf.append(pos)
          value_buf.append(value)
          has_feature = true
        }
      }
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    for (fea <- feature_list) {
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (encoded_map.contains(f_name)) {
          encoded_map(f_name).append(hash)
        } else {
          encoded_map(f_name) = ArrayBuffer(hash)
        }
      }
    }
  }

  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    var has_feature = false
    for (fea <- feature_list) {
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (pos_map.contains((f_index, hash))) {
          val pos = pos_map((f_index, hash))
          if (encoded_map.contains(f_name)) {
            encoded_map(f_name).append(pos)
          } else {
            encoded_map(f_name) = ArrayBuffer(pos)
          }
          has_feature = true
        }
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
    for (i <- feature_list.indices) {
      val raw_fea = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (pos_map.contains((f_index, hash))) {
          raw_buf.append(raw_fea)
          pos_buf.append(pos_map((f_index, hash)).toLong)
          value_buf.append(value)
        }
      }
    }
    columns.put(f_name + "_raw", raw_buf.toSeq)
    columns.put(f_name + "_index", pos_buf.toSeq)
    columns.put(f_name + "_value", value_buf.toSeq)
    pos_buf.length > 1
  }
}
