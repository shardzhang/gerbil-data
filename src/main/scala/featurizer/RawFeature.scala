package featurizer

import scala.collection.mutable.ArrayBuffer

/**
 * Feature field type constants and base class for all feature encoders.
 *
 * Two feature field types are supported:
 *  - Categorical (type=1): discrete IDs hashed into embedding indices via MurmurHash3
 *  - Continuous (type=0): numerical values used directly as embedding positions (identity mapping)
 *
 * All features produce an embedding-compatible triple of (raw, index, value) tensors.
 */
object FieldType {
  val Continuous: Byte = 0
  val Categorical: Byte = 1
}

/**
 * Base class for all feature types in the encoding framework.
 *
 * Each feature is identified by a unique (index, name) pair and produces
 * hash-based position indices for embedding lookup. Subclasses implement
 * the specific parsing and encoding logic.
 *
 * @param f_i unique feature field index within the feature set
 * @param f_n human-readable feature field name (used as TFRecord field prefix)
 * @param f_t feature type: FeatureType.Continuous (0) or FeatureType.Categorical (1)
 */
abstract class RawFeature(f_i: Int, f_n: String, f_t: Byte) extends Serializable {
  /** feature field index (unique within the feature set). 在模型训练框架(tf/pytorch)中作为lookup词表的key, 用于查询特征域的emb词表. 可重复, 重复时表示多个特征域共享同一个特征词表 */
  val field_index: Int = f_i
  /** Human-readable field name, 在模型训练框架(tf/pytorch)中作为DataSet字典的key, 用于喂入训练. 是特征域的唯一标识, 不可重复*/
  val field_name: String = f_n
  /** Field type: Continuous (0) or Categorical (1). */
  var field_type: Byte = f_t
  /** Seed for MurmurHash3 (used in categorical feature hashing). */
  final val SEED: Int = 0x3c074a61

  /** Computes raw hash values for all values in this field. */
  def getHash(dim: Long): ArrayBuffer[Long]

  /**
   * Computes detailed hash info: (f_name, f_index, f_type, raw_feature, hash, value) for each value.
   *
   * @param dim 哈希空间大小
   * @return (f_name, f_index, f_type, raw, hash, value), 其中raw表示原始特值值, hash表示经过特征工程+哈希后的值, value表示权重(for离散型特征)或原始值(for数值型特征)
   */
  def getHashInfo(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)]
}
