package featurizer

import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Continuous feature encoder for numerical values.
 *
 * Unlike categorical features (which hash discrete IDs), continuous features
 * use the raw numerical value index directly as the embedding position (identity mapping, 此时入参pos_map实际无效, 仅用作占位符).
 * This preserves the ordinal relationship between values in embedding space.
 *
 * Each feature produces three TFRecord fields:
 *  - `{field_name}_raw`: the original string representation
 *  - `{field_name}_index`: the index itself (used as embedding position, 单值特征为1L, 多值特征为从1开始的下标)
 *  - `{field_name}_value`: the numerical value (used as embedding weight)
 *
 * @tparam T the raw sample type from which this feature is extracted
 */
abstract class ContinuousFeature[T](f_i: Int, f_n: String, f_t: Byte = FieldType.Continuous) extends RawFeature(f_i, f_n, f_t) {

  /** Parses the sample and populates raw/feature/value buffers. */
  def parse(sample: T): RawFeature

  /** Raw string values for each feature. */
  var raw_list: ArrayBuffer[String] = new ArrayBuffer[String]()
  /** Position markers (typically 1L for single-valued continuous features). */
  var feature_list: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  /** Numerical values. */
  var value_list: ArrayBuffer[Float] = new ArrayBuffer[Float]()

  /** Clears all parsed buffers for reuse across samples. */
  def clear(): Unit = {
    raw_list.clear()
    feature_list.clear()
    value_list.clear()
  }

  // fixme:
  override def getHash(dim: Long): ArrayBuffer[Long] = {
    val pos_list = new ArrayBuffer[Long]()
    for (fea <- feature_list) {
      if (fea != 0) {
        val hash = fea // 对于连续新特征, fea即为hash, 两者一一映射
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
        val hash = fea // 对于连续新特征, fea即为hash, 两者一一映射
        pos_info_list.append((field_name, field_index, field_type, fmt, hash, value))
      }
    }
    pos_info_list
  }

  /** Adds raw/feature/value to a TF Example (no pos-map vocabulary lookup). */
  def add(builder: Example.Builder): Unit = {
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
        val hash = fea // 对于连续新特征, fea即为hash, 两者一一映射
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

  /** Adds raw/feature/value to a TF Example with pos-map lookup. Returns true if any feature survived filtering. */
  def add(builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
      add(builder)
      feature_list.exists(_ != 0)
  }

  /** Adds feature indices to an encoded map (no pos-map vocabulary lookup). */
  def add(encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    val pos_buf = new ArrayBuffer[Long]()
    for (fea <- feature_list) {
      if (fea != 0) {
        val hash = fea
        pos_buf.append(hash)
      }
    }
    if (pos_buf.nonEmpty) {
      encoded_map(field_name) = pos_buf
    }
  }

  /** Adds feature indices to an encoded map with pos-map vocabulary lookup. Returns true if any feature survived filtering. */
  def add(encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    add(encoded_map)
    feature_list.exists(_ != 0)
  }

  /** Adds raw/position/value arrays to a Parquet columns map with pos-map vocabulary lookup. Returns true if any feature survived filtering. */
  def add(pos_map: collection.Map[(Int, Long), Int], columns: mutable.Map[String, Any]): Boolean = {
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
      val hash = fea
      if (fea != 0) {
        raw_buf.append(raw)
        pos_buf.append(hash)
        value_buf.append(value)
      }
    }
    columns.put(field_name + "_raw", raw_buf)
    columns.put(field_name + "_index", pos_buf)
    columns.put(field_name + "_value", value_buf)
    pos_buf.length > 1
  }
}
