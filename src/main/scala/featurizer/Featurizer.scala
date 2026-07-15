package featurizer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.tensorflow.example.Example
import pipeline.serde.ParquetRecordData.Builder


/**
 * Orchestrates feature encoding across categorical, continuous, and cross features.
 *
 * Typical usage:
 * {{{
 *   val featurizer = new ML1MFeaturizer().setup()
 *   val builder = Example.newBuilder()
 *   featurizer.encode(sample, maxDim, builder)
 *   val example = builder.build()
 * }}}
 *
 * Supports two encoding modes:
 *  1. Hash-only: embeds features via MurmurHash3 (no vocabulary needed)
 *  2. PosMap vocabulary lookup: looks up pre-built vocabulary positions (production serving)
 *
 * @tparam T the raw sample type
 */
abstract class Featurizer[T] extends Serializable {
  /** Registered categorical (discrete) features. */
  val raw_cate_features: ArrayBuffer[CategoricalFeature[T]] = new ArrayBuffer[CategoricalFeature[T]]()
  /** Registered continuous (numerical) features. */
  val raw_conti_features: ArrayBuffer[ContinuousFeature[T]] = new ArrayBuffer[ContinuousFeature[T]]()
  /** Registered cross (feature-combination) features. */
  val cross_features: ArrayBuffer[CrossFeature[T]] = new ArrayBuffer[CrossFeature[T]]()
  /** The prediction target extractor. */
  var target: RawTarget[T] = _

  /** Initializes and registers all feature extractors. Must be called before encoding. */
  def setup(): Featurizer[T]

  /** Returns (field_name, field_index) pairs for all registered features. */
  def getFieldInfo(): ArrayBuffer[(String, Int)] = {
    val buff = new ArrayBuffer[(String, Int)]()
    for (raw_f <- raw_cate_features) {
      buff.append((raw_f.field_name, raw_f.field_index))
    }
    for (raw_f <- raw_conti_features) {
      buff.append((raw_f.field_name, raw_f.field_index))
    }
    for (cross_f <- cross_features) {
      buff.append((cross_f.field_name, cross_f.field_index))
    }
    buff
  }

  /** Returns the full list of Parquet column names (target + all feature fields). */
  def get_parquet_column_names(): ArrayBuffer[String] = {
    val buff = new ArrayBuffer[String]()
    buff.append("target")
    for ((f_name, f_index) <- getFieldInfo()) {
      buff.append(f_name)
    }
    buff
  }

  /** Computes raw hash values for all features (used for debugging / hash-based vocabulary). */
  def getHash(input: T, dim: Long): ArrayBuffer[Long] = {
    val buf = ArrayBuffer[Long]()
    for (raw_f <- raw_cate_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_conti_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_cate_features) {
      val hash = raw_f.getHash(dim)
      buf.appendAll(hash)
    }
    for (raw_f <- raw_conti_features) {
      val hash = raw_f.getHash(dim)
      buf.appendAll(hash)
    }
    for (cross_f <- cross_features) {
      val hash = cross_f.getHash(dim)
      buf.appendAll(hash)
    }
    buf
  }

  /** Computes detailed hash info (field name, index, type, raw, hash, value) for vocabulary building. */
  def getHashInfo(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val buf = new ArrayBuffer[(String, Int, Byte, String, Long, Float)]
    for (raw_f <- raw_cate_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_conti_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_cate_features) {
      val hash_info = raw_f.getHashInfo(dim)
      buf.appendAll(hash_info)
    }
    for (raw_f <- raw_conti_features) {
      val pos_info = raw_f.getHashInfo(dim)
      buf.appendAll(pos_info)
    }
    for (cross_f <- cross_features) {
      val pos_info = cross_f.getHashInfo(dim)
      buf.appendAll(pos_info)
    }
    buf
  }

  /** Encodes a sample into a TF Example builder using raw hashing (no pos-map vocabulary lookup). */
  def encode(input: T, dim: Long, builder: Example.Builder): Unit = {
    for (raw_f <- raw_cate_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_conti_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    target.parse(input).add(builder)

    for (raw_f <- raw_cate_features) {
      raw_f.add(dim, builder)
    }
    for (raw_f <- raw_conti_features) {
      raw_f.add(builder)
    }
    for (cross_f <- cross_features) {
      cross_f.add(dim, builder)
    }
  }

  /** Encodes a sample into a TF Example builder with pos-map vocabulary lookup. Returns (has_feature, has_target). */
  def encode(input: T, dim: Long, builder: Example.Builder, posMap: collection.Map[(Int, Long), Int], targetMap: collection.Map[Int, Int]): (Boolean, Boolean) = {
    for (raw_f <- raw_cate_features) {
      raw_f.clear()
      raw_f.parse(input)
    }

    for (raw_f <- raw_conti_features) {
      raw_f.clear()
      raw_f.parse(input)
    }

    val has_target = target
      .parse(input)
      .add(builder, targetMap)

    var has_feature = false
    for (raw_f <- raw_cate_features) {
      if (raw_f.add(dim, builder, posMap)) {
        has_feature = true
      }
    }
    for (raw_f <- raw_conti_features) {
      if (raw_f.add(builder, posMap)) {
        has_feature = true
      }
    }
    for (cross_f <- cross_features) {
      if (cross_f.add(dim, builder, posMap)) {
        has_feature = true
      }
    }
    (has_feature, has_target)
  }

  /** Encodes a sample into a delimited string format (for debugging or text-based output). */
  def encode(input: T, dim: Long, Sep1: String, Sep2: String): String = {
    val encoded_map: mutable.HashMap[String, ArrayBuffer[Long]] = new mutable.HashMap[String, ArrayBuffer[Long]]()

    for (raw_f: CategoricalFeature[T] <- raw_cate_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f: ContinuousFeature[T] <- raw_conti_features) {
      raw_f.clear()
      raw_f.parse(input)
    }

    target.parse(input)

    for (raw_f: CategoricalFeature[T] <- raw_cate_features) {
      raw_f.add(dim, encoded_map)
    }
    for (raw_f: ContinuousFeature[T] <- raw_conti_features) {
      raw_f.add(encoded_map)
    }
    for (cross_f: CrossFeature[T] <- cross_features) {
      cross_f.add(dim, encoded_map)
    }

    val buff = new ArrayBuffer[(String, Long)]()
    for ((k, vals) <- encoded_map) {
      for (v <- vals) {
        buff.append((k, v))
      }
    }
    buff.map(t => t._1 + Sep2 + t._2).mkString(Sep1)
  }

  /** Encodes a sample into a ParquetRecord builder by filling a columns map. Returns (has_feature, has_target). */
  def encode(input: T, dim: Long, parquet_builder: Builder, pos_map: collection.Map[(Int, Long), Int], target_map: collection.Map[Int, Int]): (Boolean, Boolean) = {
    for (raw_f <- raw_cate_features) {
      raw_f.clear();
      raw_f.parse(input)
    }
    for (raw_f <- raw_conti_features) {
      raw_f.clear();
      raw_f.parse(input)
    }

    val columns = new mutable.HashMap[String, Any]()
    val has_target = target
      .parse(input)
      .add(columns, target_map)

    var has_feature = false
    for (raw_f <- raw_cate_features) {
      if (raw_f.add(dim, pos_map, columns)) {
        has_feature = true
      }
    }
    for (raw_f <- raw_conti_features) {
      if (raw_f.add(pos_map, columns)) {
        has_feature = true
      }
    }
    for (cross_f <- cross_features) {
      if (cross_f.add(dim, pos_map, columns)) {
        has_feature = true
      }
    }
    parquet_builder.clear()
    parquet_builder.putAll(columns)
    (has_feature, has_target)
  }
}
