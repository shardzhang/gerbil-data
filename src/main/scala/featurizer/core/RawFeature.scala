package featurizer.core

import scala.collection.mutable.ArrayBuffer

/**
 * Feature type constants and base class for all feature encoders.
 *
 * Two feature types are supported:
 *  - Categorical (type=1): discrete IDs hashed into embedding indices via MurmurHash3
 *  - Continuous (type=0): numerical values used directly as embedding positions (identity mapping)
 *
 * All features produce an embedding-compatible triple of (raw, index, value) tensors.
 */
object FeatureType {
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
 * @param f_i unique numeric feature index within the feature set
 * @param f_n human-readable feature name (used as TFRecord field prefix)
 * @param f_t feature type: FeatureType.Continuous (0) or FeatureType.Categorical (1)
 */
abstract class RawFeature(f_i: Int, f_n: String, f_t: Byte) extends Serializable {
  /** Numeric feature index (unique within the feature set). */
  val f_index: Int = f_i
  /** Human-readable feature name. */
  val f_name: String = f_n
  /** Feature type: Continuous (0) or Categorical (1). */
  var f_type: Byte = f_t
  /** Seed for MurmurHash3 (used in categorical feature hashing). */
  final val SEED: Int = 0x3c074a61

  /** Computes raw hash values for all values in this feature. */
  def get_hash(dim: Long): ArrayBuffer[Long]

  /** Computes detailed hash info: (name, index, type, raw, hash, value) for each value. */
  def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)]
}
