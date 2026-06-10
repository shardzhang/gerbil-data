package encoder.vectorizer

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.tensorflow.example.Example

import tfrecords.serde.BytesListFeatureEncoder
import tfrecords.serde.{FloatListFeatureEncoder, Int64ListFeatureEncoder}
import utils.MurmurHash3.LongPair
import utils.MurmurHash3
import utils.ParquetRecord.ParquetRecordBuilder

/**
 * @author shard zhang
 * @date 2026/6/1 20:28
 * @note
 *
 *  1. get: 计算位置值
 *  2. 重新编码:
 *    - 类别/交叉特征: 为每一个hash值从0开始分配一个连续的唯一位置
 *    - 连续特征: 保留 feature_list 中提供的局部维度编号, 以维持多值向量的维度顺序
 *  3. add: 再次用与get同样逻辑计算位置值, 然后根据pos_map直接查询重编码后的pos值
 */
object FeatureType {
  val Continuous: Byte = 0
  val Categorical: Byte = 1
}

abstract class RawTarget[T] {
  var target: Float = 0.0F

  def parse(input: T): RawTarget[T]

  def add(builder: Example.Builder): Unit = {
    builder.getFeaturesBuilder
      .putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
  }

  /**
   *
   * @param builder
   * @param target_map raw target to encoded target
   * @return
   */
  def add(builder: Example.Builder, target_map: collection.Map[Int, Int]): Boolean = {
    if (target_map == null) {
      builder.getFeaturesBuilder
        .putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
      return true
    }
    if (target_map.contains(target.toInt)) {
      builder.getFeaturesBuilder.putFeature(
        "target", FloatListFeatureEncoder.encode(Seq(target_map(target.toInt).toFloat))
      )
      return true
    }
    false
  }

  def add(map: mutable.Map[String, Any], target_map: collection.Map[Int, Int]): Boolean = {
    if (target_map == null) {
      map.put("target", target)
      return true
    }
    if (target_map.contains(target.toInt)) {
      map.put("target", target_map(target.toInt).toFloat)
      return true
    }
    false
  }
}

abstract class RawFeature(f_i: Int, f_n: String, f_t: Byte) {
  val f_index: Int = f_i

  val f_name: String = f_n

  // 0: continuous, 1: categorical
  var f_type: Byte = f_t

  final val SEED: Int = 0x3c074a61

  /**
   * get_pos
   *
   * @param dim
   * @return List[hash]
   */
  def get_hash(dim: Long): ArrayBuffer[Long]

  /**
   * get_pos_info
   *
   * @param dim
   * @return List[(f_name, f_index, format, hash, value)]
   */
  def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)]
}

abstract class ContinuousFeature[T](f_i: Int, f_n: String, f_t: Byte = FeatureType.Continuous) extends RawFeature(f_i, f_n, f_t) {

  def parse(intput: T): RawFeature

  /**
   * 特征容器: 支持单值特征, 多值特征
   */
  // 原始特征值 (明文字符串)
  var raw_list: ArrayBuffer[String] = new ArrayBuffer[String]()

  // 原始特征值 + 简单分桶或变换 (整型)
  var feature_list: ArrayBuffer[Long] = new ArrayBuffer[Long]()

  // 原始特征值对应factor (浮点型)
  // 离散特征: 频次/权重
  // 连续特征: 自身值
  var value_list: ArrayBuffer[Float] = new ArrayBuffer[Float]()

  def clear(): Unit = {
    raw_list.clear()
    feature_list.clear()
    value_list.clear()
  }

  /**
    * 连续特征直接使用 feature_list 中提供的局部维度编号, 不再额外 hash.
    * 对多值向量特征, 这个局部维度编号就是语义维度, 重编码时必须保持不变.
    */
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

  /**
   * TFRecord
   *
   * @param dim hash space dimension
   * @param builder
   */
  def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    // raw_buf pos_buf value_buf 三者size不等则抛出异常
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

  def add(dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    if (pos_map == null) {
      add(dim, builder)
      return feature_list.exists(_ != 0)
    }

    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 连续特征值, for model
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

  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
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

  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    if (pos_map == null) {
      add(dim, encoded_map)
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
      if (fea != 0 && pos_map.contains((f_index, fea))) {
        raw_buf.append(raw_fea)
        pos_buf.append(pos_map((f_index, fea)).toLong)
        value_buf.append(value)
      }
    }
    columns.put(f_name + "_raw", raw_buf.map(_.getBytes(UTF_8)))
    columns.put(f_name + "_index", pos_buf)
    columns.put(f_name + "_value", value_buf)
    pos_buf.length > 1
  }
}

abstract class CategoricalFeature[T](f_i: Int, f_n: String, f_t: Byte = FeatureType.Categorical) extends RawFeature(f_i, f_n, f_t) {

  def parse(intput: T): RawFeature

  /**
   * 特征容器: 支持单值特征, 多值特征
   */
  // 原始特征值 (明文字符串)
  var raw_list: ArrayBuffer[String] = new ArrayBuffer[String]()

  // 原始特征值 + 简单分桶或变换 (整型)
  var feature_list: ArrayBuffer[Long] = new ArrayBuffer[Long]()

  // 原始特征值对应factor (浮点型)
  // 离散特征: 频次/权重
  // 连续特征: 自身值
  var value_list: ArrayBuffer[Float] = new ArrayBuffer[Float]()

  val key_len: Int = 4 + 8

  // ByteBuffer默认BIG_ENDIAN, 二进制存储TFRecord/Protobuf皆为LITTLE_ENDIAN
  val bytebuf: ByteBuffer = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)

  def clear(): Unit = {
    raw_list.clear()
    feature_list.clear()
    value_list.clear()
  }

  def computeHash(fea: Long, dim: Long): Long = {
    bytebuf.clear()
    bytebuf.putInt(0, f_index)
    bytebuf.putLong(4, fea)
    val p: LongPair = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  /**
   *
   * @param dim
   * @return List[hash]
   */
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

  /**
   *
   * @param dim
   * @return List[(f_name, f_index, f_type, format, hash)]
   */
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

  /**
   * TFRecord
   *
   * @param dim hash space dimension
   * @param builder
   */
  def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
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

  /**
   * TFRecord
   *
   * @param dim     hash space dimension
   * @param builder
   * @param pos_map ((f_index, hash), pos)
   * @return has_feature
   */
  def add(dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
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

  /**
   * CSV
   *
   * @param dim
   * @param encoded_map
   */
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

  /**
   * CSV
   *
   * @param dim     hash space dimension
   * @param encoded_map
   * @param pos_map (f_name, List[pos])
   * @return
   */
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
    columns.put(f_name + "_raw", raw_buf.toSeq.map(_.getBytes(UTF_8)))
    columns.put(f_name + "_index", pos_buf.toSeq)
    columns.put(f_name + "_value", value_buf.toSeq)
    pos_buf.length > 1
  }
}

class CrossFeature[T](f_i: Int, f_n: String, rnfs: CategoricalFeature[T]*) extends RawFeature(f_i, f_n, f_t = FeatureType.Categorical) {
  val indexes: Array[Int] = new Array[Int](rnfs.length)

  val key_len: Int = (4 + 8) * rnfs.length

  val bytebuf: ByteBuffer = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)

  def computeHash(dim: Long): Long = {
    bytebuf.clear()
    var shift = 0
    for (i <- 0 until rnfs.length) {
      bytebuf.putInt(shift, rnfs(i).f_index)
      shift += 4
      bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
      shift += 8
    }
    val p: LongPair = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  // body: => Unit（by-name parameter, 每次循环都重新执行 body）
  def foreachCombination(body: => Unit): Unit = {
    // https://zhuanlan.zhihu.com/p/661834313

    for (i <- 0 until rnfs.length) {
      indexes(i) = 0
    }
    var done = false
    while (!done) {
      var skip = false
      for (i <- 0 until rnfs.length) {
        if (rnfs(i).feature_list.isEmpty || rnfs(i).feature_list(indexes(i)) == 0) {
          skip = true
        }
      }
      if (!skip) {
        body
      }

      var i = rnfs.length - 1
      var added = false
      while (!added && i >= 0) {
        if (indexes(i) == rnfs(i).feature_list.length - 1) {
          indexes(i) = 0
          i -= 1
        } else {
          indexes(i) += 1
          added = true
        }
      }
      if (!added) {
        done = true
      }
    }
  }

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  override def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val buf = ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
    foreachCombination {
      var fmt: String = ""
      for (i <- 0 until rnfs.length) {
        fmt += rnfs(i).f_index.toString + ":" + rnfs(i).raw_list(indexes(i))
        if (i < rnfs.length - 1) fmt += "__xx__"
      }
      buf.append((f_name, f_index, f_type, fmt, computeHash(dim), 1.0F))
    }
    buf
  }

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  def get_hash_info(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    get_hash_info(dim)
  }

  /**
   *
   * @param dim
   * @return ArrayBuffer[hash]
   */
  override def get_hash(dim: Long): ArrayBuffer[Long] = {
    val buf = new ArrayBuffer[Long]
    foreachCombination {
      buf.append(computeHash(dim))
    }
    buf
  }

  /**
   * TFRecord
   *
   * @param dim hash space dimension
   * @param builder
   */
  def add(dim: Long, builder: Example.Builder): Unit = {
    // https://zhuanlan.zhihu.com/p/661834313
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    foreachCombination {
      var fmt: String = ""
      for (i <- 0 until rnfs.length) {
        fmt += rnfs(i).f_index.toString + ":" + rnfs(i).raw_list(indexes(i))
        if (i < rnfs.length - 1) fmt += "__xx__"
      }
      raw_buf.append(fmt)
      pos_buf.append(computeHash(dim))
      value_buf.append(1.0F)
      has_feature = true
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  /**
   * TFRecord
   *
   * @param input
   * @param dim
   * @param builder
   */
  def add(input: T, dim: Long, builder: Example.Builder): Unit = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    add(dim, builder)
  }

  /**
   * TFRecord
   *
   * @param dim     hash space dimension
   * @param builder
   * @param pos_map ((f_index, hash), pos)
   * @return
   */
  def add(dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    // https://zhuanlan.zhihu.com/p/661834313
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    foreachCombination {
      var fmt: String = ""
      for (i <- 0 until rnfs.length) {
        fmt += rnfs(i).f_index.toString + ":" + rnfs(i).raw_list(indexes(i))
        if (i < rnfs.length - 1) fmt += "__xx__"
      }
      val hash = computeHash(dim)
      if (pos_map.contains((f_index, hash))) {
        val pos = pos_map((f_index, hash))
        raw_buf.append(fmt)
        pos_buf.append(pos)
        value_buf.append(1.0F)
        has_feature = true
      }
    }
    builder.getFeaturesBuilder
      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
      .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
      .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
    has_feature
  }

  /**
   * TFRecord
   *
   * @param input
   * @param dim
   * @param builder
   * @param pos_map Map[(f_index, hash), pos]
   * @return
   */
  def add(input: T, dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    add(dim, builder, pos_map)
  }

  /**
   * TSV
   *
   * @param dim
   * @param encoded_map
   */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    foreachCombination {
      encoded_map.getOrElseUpdate(f_name, ArrayBuffer.empty[Long]).append(computeHash(dim))
    }
  }

  /**
   * TSV
   *
   * @param dim     hash space dimension
   * @param pos_map (f_name, List[pos])
   */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    var has_feature = false
    foreachCombination {
      pos_map.get((f_index, computeHash(dim))).foreach { pos =>
        encoded_map.getOrElseUpdate(f_name, ArrayBuffer.empty[Long]).append(pos)
        has_feature = true
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
    foreachCombination {
      var fmt: String = ""
      for (i <- 0 until rnfs.length) {
        fmt += rnfs(i).f_index.toString + ":" + rnfs(i).raw_list(indexes(i))
        if (i < rnfs.length - 1) fmt += "__xx__"
      }
      val hash = computeHash(dim)
      if (pos_map.contains((f_index, hash))) {
        raw_buf.append(fmt)
        pos_buf.append(pos_map((f_index, hash)).toLong)
        value_buf.append(1.0F)
      }
    }
    columns.put(f_name + "_raw", raw_buf.toSeq.map(_.getBytes(UTF_8)))
    columns.put(f_name + "_index", pos_buf.toSeq)
    columns.put(f_name + "_value", value_buf.toSeq)
    pos_buf.length > 1
  }
}

abstract class FeatureEncoder[T] {
  val raw_cate_features: ArrayBuffer[CategoricalFeature[T]] = new ArrayBuffer[CategoricalFeature[T]]()
  val raw_conti_features: ArrayBuffer[ContinuousFeature[T]] = new ArrayBuffer[ContinuousFeature[T]]()
  val cross_features: ArrayBuffer[CrossFeature[T]] = new ArrayBuffer[CrossFeature[T]]()
  var target: RawTarget[T] = _

  def setup(): FeatureEncoder[T]

  /**
   *
   * @return List[(f_name, f_index)]
   */
  def get_field_name_and_index(): ArrayBuffer[(String, Int)] = {
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

  /**
   * ParquetRecord
   *
   * @return
   */
  def get_parquet_column_names(): ArrayBuffer[String] = {
    val buff = new ArrayBuffer[String]()
    buff.append("target")
    for ((f_name, f_index) <- get_field_name_and_index()) {
      buff.append(f_name)
    }
    buff
  }

  /**
   *
   * @param input
   * @param dim
   * @return List[pos]
   */
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

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, f_type, format, pos)]
   */
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

  /**
   * TFRecord
   *
   * @param input
   * @param dim
   * @param builder
   */
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
      raw_f.add(dim, builder)
    }
    for (cross_f <- cross_features) {
      cross_f.add(dim, builder)
    }
  }

  /**
   * TFRecord
   *
   * @param input
   * @param dim
   * @param builder
   * @param pos_map
   * @param target_map
   * @return
   */
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
      if (raw_f.add(dim, builder, pos_map)) {
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

  /**
   * TSV
   *
   * @param input
   * @param dim
   * @param Sep1
   * @param Sep2
   * @return
   */
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

    target.parse(input) // fixme

    for (raw_f: CategoricalFeature[T] <- raw_cate_features) {
      raw_f.add(dim, encoded_map)
    }
    for (raw_f: ContinuousFeature[T] <- raw_conti_features) {
      raw_f.add(dim, encoded_map)
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

  /**
   * ParquetRecord
   *
   * @param input
   * @param dim
   * @param pos_map
   * @param target_map
   * @return
   */
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
      if (raw_f.add(dim, pos_map, columns)) {
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
