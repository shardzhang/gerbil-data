package featurizer.core

import scala.collection.mutable.ArrayBuffer

object FeatureType {
  val Continuous: Byte = 0
  val Categorical: Byte = 1
}

abstract class RawFeature(f_i: Int, f_n: String, f_t: Byte) extends Serializable {
  val f_index: Int = f_i

  val f_name: String = f_n

  var f_type: Byte = f_t

  final val SEED: Int = 0x3c074a61

  def get_hash(dim: Long): ArrayBuffer[Long]

  def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)]
}
