package featurizer.core

import org.tensorflow.example.Example
import pipeline.serde.ParquetRecord.ParquetRecordBuilder
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** We use a featurizer to convert raw samples into featurized vectors. */
abstract class Featurizer[T] extends Serializable {
  val raw_cate_features: ArrayBuffer[CategoricalFeature[T]] = new ArrayBuffer[CategoricalFeature[T]]()
  val raw_conti_features: ArrayBuffer[ContinuousFeature[T]] = new ArrayBuffer[ContinuousFeature[T]]()
  val cross_features: ArrayBuffer[CrossFeature[T]] = new ArrayBuffer[CrossFeature[T]]()
  var target: RawTarget[T] = _

  def setup(): Featurizer[T]

  def getFieldInfo(): ArrayBuffer[(String, Int)] = {
    val buff = new ArrayBuffer[(String, Int)]()
    for (raw_f <- raw_cate_features) {
      buff.append((raw_f.f_name, raw_f.f_index))
    }
    for (raw_f <- raw_conti_features) {
      buff.append((raw_f.f_name, raw_f.f_index))
    }
    for (cross_f <- cross_features) {
      buff.append((cross_f.f_name, cross_f.f_index))
    }
    buff
  }

  def get_parquet_column_names(): ArrayBuffer[String] = {
    val buff = new ArrayBuffer[String]()
    buff.append("target")
    for ((f_name, f_index) <- getFieldInfo()) {
      buff.append(f_name)
    }
    buff
  }

  def get_hash(input: T, dim: Long): ArrayBuffer[Long] = {
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
      val hash = raw_f.get_hash(dim)
      buf.appendAll(hash)
    }
    for (raw_f <- raw_conti_features) {
      val hash = raw_f.get_hash(dim)
      buf.appendAll(hash)
    }
    for (cross_f <- cross_features) {
      val hash = cross_f.get_hash(dim)
      buf.appendAll(hash)
    }
    buf
  }

  def get_hash_info(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
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
      val hash_info = raw_f.get_hash_info(dim)
      buf.appendAll(hash_info)
    }
    for (raw_f <- raw_conti_features) {
      val pos_info = raw_f.get_hash_info(dim)
      buf.appendAll(pos_info)
    }
    for (cross_f <- cross_features) {
      val pos_info = cross_f.get_hash_info(dim)
      buf.appendAll(pos_info)
    }
    buf
  }

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

  def encode(input: T, dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int], target_map: collection.Map[Int, Int]): (Boolean, Boolean) = {
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
      .add(builder, target_map)

    var has_feature = false
    for (raw_f <- raw_cate_features) {
      if (raw_f.add(dim, builder, pos_map)) {
        has_feature = true
      }
    }
    for (raw_f <- raw_conti_features) {
      if (raw_f.add(builder, pos_map)) {
        has_feature = true
      }
    }
    for (cross_f <- cross_features) {
      if (cross_f.add(dim, builder, pos_map)) {
        has_feature = true
      }
    }
    (has_feature, has_target)
  }

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

  def encode(input: T, dim: Long, parquet_builder: ParquetRecordBuilder, pos_map: collection.Map[(Int, Long), Int], target_map: collection.Map[Int, Int]): (Boolean, Boolean) = {
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
