package featurizer

import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}
import utils.MurmurHash3

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
 * Categorical feature encoder for discrete IDs.
 *
 * Encodes each discrete feature value into an embedding index via MurmurHash3.
 * The hash key is `(f_index || feature_value)` in little-endian byte order,
 * which ensures different fields with the same raw value map to different
 * embedding positions.
 *
 * Each feature produces three TFRecord fields:
 *  - `{field_name}_raw`: the original string value
 *  - `{field_name}_index`: the hashed embedding position
 *  - `{field_name}_value`: the weight/importance (typically 1.0 for categorical)
 *
 * @tparam T the raw sample type from which this feature is extracted
 */
abstract class CategoricalFeature[T](f_i: Int, f_n: String, f_t: Byte = FieldType.Categorical) extends RawFeature(f_i, f_n, f_t) {

  /** Parses the sample and populates raw/feature/value buffers. */
  def parse(sample: T): RawFeature

  /** Raw string values for each occurrence. */
  var raw_list: ArrayBuffer[String] = new ArrayBuffer[String]()
  /** Discrete IDs for each occurrence (used as input to hash). */
  var feature_list: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  /** Weights/values for each ID. */
  var value_list: ArrayBuffer[Float] = new ArrayBuffer[Float]()
  /** Byte length of the hash key: 4 (f_index) + 8 (feature). */
  val key_len: Int = 4 + 8

  /** Clears all parsed buffers for reuse across samples. */
  def clear(): Unit = {
    raw_list.clear()
    feature_list.clear()
    value_list.clear()
  }

  /** Computes the hash of a feature value using MurmurHash3 with (f_index || feature) as the key. */
  def computeHash(fea: Long, dim: Long): Long = {
    val bb = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(0, field_index)
    bb.putLong(4, fea)
    val p = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(bb.array(), 0, key_len, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  // fixme:
  override def getHash(dim: Long): ArrayBuffer[Long] = {
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

  // fixme:
  override def getHashInfo(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val pos_info_list = new ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
    for (i <- feature_list.indices) {
      val raw = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val fmt = field_index.toString + ":" + raw
        val hash = computeHash(fea, dim)
        pos_info_list.append((field_name, field_index, field_type, fmt, hash, value))
      }
    }
    pos_info_list
  }

  /** Adds raw/feature/value to a TF Example (no pos-map vocabulary lookup). */
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
      val raw = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        raw_buf.append(raw)
        pos_buf.append(hash)
        value_buf.append(value)
      }
    }
    builder.getFeaturesBuilder
      .putFeature(field_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(field_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(field_name + "_value", FloatListFeatureEncoder.encode(value_buf))
  }

  /** Adds raw/feature/value to a TF Example with pos-map vocabulary lookup. Returns true if any feature survived filtering. */
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
      val raw = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (pos_map.contains((field_index, hash))) {
          raw_buf.append(raw)
          val pos = pos_map((field_index, hash))
          pos_buf.append(pos)
          value_buf.append(value)
          has_feature = true
        }
      }
    }
    builder.getFeaturesBuilder
      .putFeature(field_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(field_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(field_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  /** Adds hashed positions to an encoded map (no pos-map vocabulary lookup). */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    for (fea <- feature_list) {
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (encoded_map.contains(field_name)) {
          encoded_map(field_name).append(hash)
        } else {
          encoded_map(field_name) = ArrayBuffer(hash)
        }
      }
    }
  }

  /** Adds positions to an encoded map with pos-map vocabulary lookup. Returns true if any feature survived filtering. */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    var has_feature = false
    for (fea <- feature_list) {
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (pos_map.contains((field_index, hash))) {
          val pos = pos_map((field_index, hash))
          if (encoded_map.contains(field_name)) {
            encoded_map(field_name).append(pos)
          } else {
            encoded_map(field_name) = ArrayBuffer(pos)
          }
          has_feature = true
        }
      }
    }
    has_feature
  }

  /** Adds raw/position/value arrays to a Parquet columns map with pos-map vocabulary lookup. Returns true if any feature survived filtering. */
  def add(dim: Long, pos_map: collection.Map[(Int, Long), Int], columns: mutable.Map[String, Any]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)
    for (i <- feature_list.indices) {
      val raw = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        val hash = computeHash(fea, dim)
        if (pos_map.contains((field_index, hash))) {
          raw_buf.append(raw)
          pos_buf.append(pos_map((field_index, hash)).toLong)
          value_buf.append(value)
        }
      }
    }
    columns.put(field_name + "_raw", raw_buf)
    columns.put(field_name + "_index", pos_buf)
    columns.put(field_name + "_value", value_buf)
    pos_buf.length > 1
  }
}
