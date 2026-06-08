package encoder.vectorizer
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable
import scala.collection.immutable
import scala.collection.mutable.{HashMap, ArrayBuffer}
import org.tensorflow.example.Example
import tfrecords.serde.BytesListFeatureEncoder
import tfrecords.serde.{FloatListFeatureEncoder, Int64ListFeatureEncoder}
import utils.MurmurHash3.LongPair
import utils.MurmurHash3
import utils.ParquetRecord
import utils.ParquetRecord.ParquetRecordBuilder

/**
 * @author shard zhang
 * @date 2026/6/1 20:28
 * @note
 *
 *  get: 计算hash值
 *  重新编码: 为每一个hash值从0开始分配一个连续的唯一位置, 构成pos_map: HashMap[(f_n, f_i, hash), pos]
 *  add: 再次用与get同样逻辑计算hash值, 然后根据pos_map直接查询pos值
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
  def add(builder: Example.Builder, target_map: immutable.HashMap[Int, Int]): Boolean = {
    if (target_map == null) {
      builder.getFeaturesBuilder
        .putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
      return true
    }
    if (target_map.contains(target.toInt)) {
      builder.getFeaturesBuilder.putFeature(
        "target", FloatListFeatureEncoder.encode(Seq(target_map(target.toInt)))
      )
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
  def get_pos(dim: Long): ArrayBuffer[Long]

  /**
   * get_pos_info
   *
   * @param dim
   * @return List[(f_name, f_index, format, hash, value)]
   */
  def get_pos_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)]

  /**
   * TFRecord
   *
   * @param dim hash space dimension
   * @param builder
   */
  def add(dim: Long, builder: Example.Builder): Unit

  /**
   * TFRecord
   *
   * @param dim     hash space dimension
   * @param builder
   * @param pos_map ((f_index, hash), pos)
   * @return
   */
  def add(dim: Long, builder: Example.Builder, pos_map: immutable.HashMap[(Int, Long), Int]): Boolean

  /**
   * TSV
   *
   * @param dim
   * @param encoded_map
   */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit

  /**
   * TSV
   *
   * @param dim     hash space dimension
   * @param pos_map (f_name, List[pos])
   */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean
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

  override def toString: String = {
    raw_list.mkString(",")
    feature_list.mkString(",")
    value_list.mkString(",")
  }

  /**
    * 连续特征直接使用 feature_list 中提供的局部维度编号, 不再额外 hash.
    */
  override def get_pos(dim: Long): ArrayBuffer[Long] = {
    val pos_list = new ArrayBuffer[Long]()
    for (fea <- feature_list) {
      if (fea != 0) {
        pos_list.append(fea)
      }
    }
    pos_list
  }

  override def get_pos_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
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
  override def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    // raw_buf pos_buf value_buf 三者size不等则抛出异常
    if (raw_buf.size != pos_buf.size || raw_buf.size != value_buf.size) {
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

  override def add(dim: Long, builder: Example.Builder, pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
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

  override def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
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

  override def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
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

  override def toString: String = {
    raw_list.mkString(",")
    feature_list.mkString(",")
    value_list.mkString(",")
  }

  /**
   *
   * @param dim
   * @return List[hash]
   */
  override def get_pos(dim: Long): ArrayBuffer[Long] = {
    val pos_list = new ArrayBuffer[Long]()
    for (i <- feature_list.indices) {
      val fea = feature_list(i)
      val value = value_list(i)
      val raw_fea = raw_list(i)
      if (fea != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, fea)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var pos = p.val1 % dim
        if (pos < 0) {
          pos += dim
        }
        pos_list.append(pos)
      }
    }
    pos_list
  }

  /**
   *
   * @param dim
   * @return List[(f_name, f_index, f_type, format, hash)]
   */
  override def get_pos_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val pos_info_list = new ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
    // format: f_index:raw_fea
    var fmt: String = ""
    for (i <- feature_list.indices) {
      val fea = feature_list(i)
      val value = value_list(i)
      val raw_fea = raw_list(i)
      if (fea != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        fmt += f_index.toString + ":"
        bytebuf.putLong(4, fea)
        fmt += raw_fea.toString
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var pos: Long = p.val1 % dim
        if (pos < 0) {
          pos += dim
        }
        pos_info_list.append((f_name, f_index, f_type, fmt, pos, value))
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
  override def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    // raw_buf pos_buf value_buf 三者size不等则抛出异常
    if (raw_buf.size != pos_buf.size || raw_buf.size != value_buf.size) {
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
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, fea)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
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
  override def add(dim: Long, builder: Example.Builder, pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    // raw_buf pos_buf value_buf 三者size不等则抛出异常
    if (raw_buf.size != pos_buf.size || raw_buf.size != value_buf.size) {
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
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, fea)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        if (pos_map.contains((f_index, hash))) {
          val pos = pos_map((f_index, hash))
          raw_buf.append(raw_fea)
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
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    encoded_map.clear()
    for (fea <- feature_list) {
      if (fea != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, fea)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var pos: Long = p.val1 % dim
        if (pos < 0) {
          pos += dim
        }
        if (encoded_map.contains(f_name)) {
          encoded_map(f_name).append(pos)
        } else {
          encoded_map(f_name) = ArrayBuffer(pos)
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
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
    encoded_map.clear()

    val pos_buf = new ArrayBuffer[Long]()
    pos_buf.append(0L)
    var has_feature = false
    for (fea <- feature_list) {
      if (fea != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, fea)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
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
}

class CrossFeature[T](f_i: Int, f_n: String, rnfs: CategoricalFeature[T]*) extends RawFeature(f_i, f_n, f_t = FeatureType.Categorical) {
  val indexes: Array[Int] = new Array[Int](rnfs.length)

  val key_len: Int = (4 + 8) * rnfs.length

  val bytebuf: ByteBuffer = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  override def get_pos_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    // https://zhuanlan.zhihu.com/p/661834313
    val buf = ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
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
        bytebuf.clear()
        var shift = 0
        // format: f_index:f_value__xx__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4
          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__xx__"
        }
        fmt = fmt.stripSuffix("__xx__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        buf.append((f_name, f_index, f_type, fmt, hash, 1.0F))
      }

      var pos = rnfs.length - 1
      var added = false
      while (!added && pos >= 0) {
        if (indexes(pos) == rnfs(pos).feature_list.length - 1) {
          indexes(pos) = 0
          pos -= 1
        } else {
          indexes(pos) += 1
          added = true
        }
      }
      if (!added) {
        done = true
      }
    }
    buf
  }

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  def get_pos_info(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    get_pos_info(dim)
  }

  /**
   *
   * @param dim
   * @return ArrayBuffer[hash]
   */
  override def get_pos(dim: Long): ArrayBuffer[Long] = {
    // https://zhuanlan.zhihu.com/p/661834313
    val buf = new ArrayBuffer[Long]
    for (i <- indexes.indices) {
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
        bytebuf.clear()
        var shift = 0
        // format: f_index:f_value__xx__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4

          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__xx__"
        }
        fmt = fmt.stripSuffix("__xx__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        buf.append(hash)
      }

      var pos = rnfs.length - 1
      var added = false
      while (!added && pos >= 0) {
        if (indexes(pos) == rnfs(pos).feature_list.length - 1) {
          indexes(pos) = 0
          pos -= 1
        } else {
          indexes(pos) += 1
          added = true
        }
      }
      if (!added) {
        done = true
      }
    }
    buf
  }

  /**
   * TFRecord
   *
   * @param dim hash space dimension
   * @param builder
   */
  override def add(dim: Long, builder: Example.Builder): Unit = {
    // https://zhuanlan.zhihu.com/p/661834313
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)
    for (i <- 0 until rnfs.length) {
      indexes(i) = 0
    }

    var has_feature = false
    var done = false
    while (!done) {
      var skip = false
      for (i <- 0 until rnfs.length) {
        if (rnfs(i).feature_list.isEmpty || rnfs(i).feature_list(indexes(i)) == 0) {
          skip = true
        }
      }
      if (!skip) {
        bytebuf.clear()
        var shift = 0
        // format: f_index:f_value__xx__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4
          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).raw_list(indexes(i)).toString
          shift += 8
          fmt += "__xx__"
        }
        fmt = fmt.stripSuffix("__xx__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var pos = p.val1 % dim
        if (pos < 0) {
          pos += dim
        }
        raw_buf.append(fmt)
        pos_buf.append(pos)
        value_buf.append(1.0F)
        has_feature = true
      }

      var pos = rnfs.length - 1
      var added = false
      while (!added && pos >= 0) {
        if (indexes(pos) == rnfs(pos).feature_list.length - 1) {
          indexes(pos) = 0
          pos -= 1
        } else {
          indexes(pos) += 1
          added = true
        }
      }
      if (!added) {
        done = true
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
  override def add(dim: Long, builder: Example.Builder, pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
    // https://zhuanlan.zhihu.com/p/661834313
    val raw_buf = new ArrayBuffer[String]()      // 原始特征值, for human debug
    val pos_buf = new ArrayBuffer[Long]()        // 编码后位置, for model
    val value_buf = new ArrayBuffer[Float]()     // 频次/权重, for model
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    for (i <- 0 until rnfs.length) {
      indexes(i) = 0
    }

    var has_feature = false
    var done = false
    while (!done) {
      var skip = false
      for (i <- 0 until rnfs.length) {
        if (rnfs(i).feature_list.isEmpty || rnfs(i).feature_list(indexes(i)) == 0) {
          skip = true
        }
      }

      if (!skip) {
        bytebuf.clear()
        var shift = 0
        // format: f_index:f_value__xx__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4
          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__xx__"
        }
        fmt = fmt.stripSuffix("__xx__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        if (pos_map.contains((f_index, hash))) {
          pos_buf.append(pos_map((f_index, hash)))
          raw_buf.append(fmt)
          value_buf.append(1.0F)
          has_feature = true
        }
      }

      var pos = rnfs.length - 1
      var added = false
      while (!added && pos >= 0) {
        if (indexes(pos) == rnfs(pos).feature_list.length - 1) {
          indexes(pos) = 0
          pos -= 1
        } else {
          indexes(pos) += 1
          added = true
        }
      }
      if (!added) {
        done = true
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
  def add(input: T, dim: Long, builder: Example.Builder, pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
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
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
  }

  /**
   * TSV
   *
   * @param dim     hash space dimension
   * @param pos_map (f_name, List[pos])
   */
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
    true
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
  def get_pos(input: T, dim: Long): ArrayBuffer[Long] = {
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
      val pos = raw_f.get_pos(dim)
      buf.appendAll(pos)
    }
    for (raw_f <- raw_conti_features) {
      val pos = raw_f.get_pos(dim)
      buf.appendAll(pos)
    }
    for (cross_f <- cross_features) {
      val pos = cross_f.get_pos(dim)
      buf.appendAll(pos)
    }
    buf
  }

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, f_type, format, pos)]
   */
  def get_pos_info(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
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
      val pos_info = raw_f.get_pos_info(dim)
      buf.appendAll(pos_info)
    }
    for (raw_f <- raw_conti_features) {
      val pos_info = raw_f.get_pos_info(dim)
      buf.appendAll(pos_info)
    }
    for (cross_f <- cross_features) {
      val pos_info = cross_f.get_pos_info(dim)
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
  def encode(input: T, dim: Long, builder: Example.Builder, pos_map: immutable.HashMap[(Int, Long), Int], target_map: immutable.HashMap[Int, Int]): (Boolean, Boolean) = {
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
  def encode(input: T, dim: Long, parquet_builder: ParquetRecordBuilder, pos_map: immutable.HashMap[(Int, Long), Int], target_map: immutable.HashMap[Int, Int]): (Boolean, Boolean) = {
    val example_builder = Example.newBuilder()
    val (has_feature, has_target) = encode(input, dim, example_builder, pos_map, target_map)
    parquet_builder.clear()
    parquet_builder.putAll(ParquetRecord.from_example(example_builder.build()).columns)
    (has_feature, has_target)
  }
}
