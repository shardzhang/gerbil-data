package featurizer.core

import java.nio.charset.StandardCharsets.UTF_8
import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** This continuous featurizer encodes numerical value into an embedding-compatible index. */
abstract class ContinuousFeature[T](f_i: Int, f_n: String, f_t: Byte = FeatureType.Continuous) extends RawFeature(f_i, f_n, f_t) {

  /** Parses the sample and populates raw/feature/value buffers. */
  def parse(sample: T): RawFeature

  /** Raw string values for each occurrence. */
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

  override def get_hash(dim: Long): ArrayBuffer[Long] = {
    val pos_list = new ArrayBuffer[Long]()
    for (fea <- feature_list) {
      if (fea != 0) {
        pos_list.append(fea)
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
        val fmt = f_index.toString + ":" + raw_fea.toString
        pos_info_list.append((f_name, f_index, f_type, fmt, fea, value))
      }
    }
    pos_info_list
  }

  /** Adds raw/feature/value tensors to a TF Example (no pos-map lookup). */
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
      val raw_fea = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0) {
        raw_buf.append(raw_fea)
        pos_buf.append(fea)
        value_buf.append(value)
      }
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
  }

  /** Adds raw/feature/value tensors to a TF Example with pos-map lookup. Returns true if any feature survived filtering. */
  def add(builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    if (pos_map == null) {
      add(builder)
      return feature_list.exists(_ != 0)
    }

    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    for (i <- feature_list.indices) {
      val raw_fea = raw_list(i)
      val fea = feature_list(i)
      val value = value_list(i)
      if (fea != 0 && pos_map.contains((f_index, fea))) {
        raw_buf.append(raw_fea)
        pos_buf.append(pos_map((f_index, fea)).toLong)
        value_buf.append(value)
        has_feature = true
      }
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  /** Adds feature indices to an encoded map (no pos-map lookup). */
  def add(encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    val pos_buf = new ArrayBuffer[Long]()
    for (fea <- feature_list) {
      if (fea != 0) {
        pos_buf.append(fea)
      }
    }
    if (pos_buf.nonEmpty) {
      encoded_map(f_name) = pos_buf
    }
  }

  /** Adds feature indices to an encoded map with pos-map lookup. Returns true if any feature survived filtering. */
  def add(encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    if (pos_map == null) {
      add(encoded_map)
      return feature_list.exists(_ != 0)
    }

    val pos_buf = new ArrayBuffer[Long]()
    var has_feature = false
    for (fea <- feature_list) {
      if (fea != 0 && pos_map.contains((f_index, fea))) {
        pos_buf.append(pos_map((f_index, fea)).toLong)
        has_feature = true
      }
    }
    if (has_feature) {
      encoded_map(f_name) = pos_buf
    }
    has_feature
  }

  /** Adds raw/position/value arrays to a Parquet columns map with pos-map lookup. Returns true if any feature survived filtering. */
  def add(pos_map: collection.Map[(Int, Long), Int], columns: mutable.Map[String, Any]): Boolean = {
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
      if (fea != 0 && pos_map.contains((f_index, fea))) {
        raw_buf.append(raw_fea)
        pos_buf.append(pos_map((f_index, fea)).toLong)
        value_buf.append(value)
      }
    }
    columns.put(f_name + "_raw", raw_buf.toSeq)
    columns.put(f_name + "_index", pos_buf)
    columns.put(f_name + "_value", value_buf)
    pos_buf.length > 1
  }
}
