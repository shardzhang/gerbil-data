package featurizer.core

import scala.collection.mutable.ArrayBuffer

/**
 * @author shard zhang
 * @date 2026/6/10 17:57
 * @note Feature type constants (Categorical=1, Continuous=0) and base feature fields
 */

/** Feature type constants distinguishing categorical (discrete) from continuous (numerical) features. */
object FeatureType {
  /** Numerical value used directly as embedding position (no hashing). */
  val Continuous: Byte = 0
  /** Discrete ID hashed into embedding space. */
  val Categorical: Byte = 1
}

/** Base class for all feature types. Each feature has an index, name, type, and produces hash-based position indices for embedding lookup. */
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
