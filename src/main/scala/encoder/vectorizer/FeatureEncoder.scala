package encoder.vectorizer

import java.nio.{ByteBuffer, ByteOrder}
import io.netty.buffer.ByteBuf
import scala.collection.mutable
import scala.collection.immutable
import scala.collection.mutable.{HashMap, ListBuffer}

import com.google.protobuf.ByteString
import org.tensorflow.example.Example
import org.tensorflow.example.Feature
import org.tensorflow.example.Features
import org.tensorflow.example.Int64List
import org.tensorflow.example.BytesList
import org.tensorflow.example.FloatList

import utils.MurmurHash3.LongPair
import utils.MurmurHash3
import utils.TFRecord
import utils.ParquetRecord

/**
 * @author shard zhang
 * @date 2026/6/1 20:28
 * @note
 */
abstract class Target[T] {
  var target: Float = 0.0F

  def parse(input: T): Target[T]

  def add(builder: Example.Builder): Unit = {
    builder.getFeaturesBuilder
      .putFeature("target", TFRecord.floatFeature(target))
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
        .putFeature("target", TFRecord.floatFeature(target))
      return true
    }
    if (target_map.contains(target.toInt)) {
      builder.getFeaturesBuilder.putFeature(
        "target", TFRecord.floatFeature(target_map(target.toInt))
      )
      return true
    }
    false
  }
}

abstract class Feature(f_i: Int, f_n: String) {
  val f_index: Int = f_i

  val f_name: String = f_n

  final val SEED: Int = 0x3c074a61

  /**
   * get_pos
   *
   * @param dim
   * @return List[hash]
   */
  def get_pos(dim: Long): ListBuffer[Long]

  /**
   * get_pos_info
   *
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  def get_pos_info(dim: Long): ListBuffer[(String, Int, String, Long)]

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
  def add(dim: Long, encoded_map: mutable.HashMap[String, ListBuffer[Long]]): Unit

  /**
   * TSV
   *
   * @param dim     hash space dimension
   * @param pos_map (f_name, List[pos])
   */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ListBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean
}

abstract class RawFeature[T](f_i: Int, f_n: String) extends Feature(f_i, f_n) {

  def parse(intput: T): Feature

  // 支持单值特征, 多值特征. 若支持带权重, 需要ListBuffer[Long]
  var feature_list: ListBuffer[Long] = new ListBuffer[Long]()

  val key_len: Int = 4 + 8

  // ByteBuffer默认BIG_ENDIAN, 二进制存储TFRecord/Protobuf皆为LITTLE_ENDIAN
  val bytebuf: ByteBuffer = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)

  def clear(): Unit = {
    feature_list.clear()
  }

  override def toString: String = {
    feature_list.mkString(",")
  }

  /**
   *
   * @param dim
   * @return List[hash]
   */
  override def get_pos(dim: Long): ListBuffer[Long] = {
    // hash
    val buf = new ListBuffer[Long]()
    for (value <- feature_list) {
      if (value != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, value)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        buf.append(hash)
      }
    }
    buf
  }

  /**
   *
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  override def get_pos_info(dim: Long): ListBuffer[(String, Int, String, Long)] = {
    val buf = new ListBuffer[(String, Int, String, Long)]()
    // format: f_index:f_value
    var fmt: String = ""
    for (value <- feature_list) {
      if (value != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        fmt += f_index.toString + ":"
        bytebuf.putLong(4, value)
        fmt += value.toString
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        buf.append((f_name, f_index, fmt, hash))
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
    val pos_buf = new ListBuffer[Long]()
    pos_buf.append(0L)
    for (value <- feature_list) {
      if (value != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, value)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        pos_buf.append(hash)
      }
    }
    builder.getFeaturesBuilder.putFeature(
      f_name + "_pos", TFRecord.int64VectorFeature(pos_buf.toArray)
    )
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
    val pos_buffer = new ListBuffer[Long]()
    pos_buffer.append(0L)
    var has_feature = false
    for (value <- feature_list) {
      if (value != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, value)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        if (pos_map.contains((f_index, hash))) {
          val pos = pos_map((f_index, hash))
          pos_buffer.append(pos)
          has_feature = true
        }
      }
    }
    builder.getFeaturesBuilder.putFeature(
      f_name + "_pos", TFRecord.int64VectorFeature(pos_buffer.toArray)
    )
    has_feature
  }

  /**
   * CSV
   *
   * @param dim
   * @param encoded_map
   */
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ListBuffer[Long]]): Unit = {
    encoded_map.clear()
    for (value <- feature_list) {
      if (value != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, value)
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash: Long = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        if (encoded_map.contains(f_name)) {
          encoded_map(f_name).append(hash)
        } else {
          encoded_map(f_name) = ListBuffer(hash)
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
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ListBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
    encoded_map.clear()

    val pos_buf = new ListBuffer[Long]()
    pos_buf.append(0L)
    var has_feature = false
    for (value <- feature_list) {
      if (value != 0) {
        bytebuf.clear()
        bytebuf.putInt(0, f_index)
        bytebuf.putLong(4, value)
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
            encoded_map(f_name) = ListBuffer(pos)
          }
          has_feature = true
        }
      }
    }
    has_feature
  }
}

class CrossFeature[T](f_i: Int, f_n: String, rnfs: RawFeature[T]*) extends Feature(f_i, f_n) {
  val indexes: Array[Int] = new Array[Int](rnfs.length)

  val key_len: Int = (4 + 8) * rnfs.length

  val bytebuf: ByteBuffer = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)

  /**
   *
   * @param input
   * @param dim
   * @return List[(f_name, f_index, format, hash)]
   */
  override def get_pos_info(dim: Long): ListBuffer[(String, Int, String, Long)] = {
    // https://zhuanlan.zhihu.com/p/661834313
    val buf = ListBuffer[(String, Int, String, Long)]()
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
        // format: f_index:f_value__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4
          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__"
        }
        fmt = fmt.stripSuffix("__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        buf.append((f_name, f_index, fmt, hash))
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
  def get_pos_info(input: T, dim: Long): ListBuffer[(String, Int, String, Long)] = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    get_pos_info(dim)
  }

  /**
   *
   * @param dim
   * @return ListBuffer[hash]
   */
  override def get_pos(dim: Long): ListBuffer[Long] = {
    // https://zhuanlan.zhihu.com/p/661834313
    val buf = new ListBuffer[Long]
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
        // format: f_index:f_value__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4

          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__"
        }
        fmt = fmt.stripSuffix("__")
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
    val pos_buffer = new ListBuffer[Long]()
    pos_buffer.append(0)

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
        // format: f_index:f_value__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4
          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__"
        }
        fmt = fmt.stripSuffix("__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        pos_buffer.append(hash)
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
    builder.getFeaturesBuilder.putFeature(
      f_name + "_pos", TFRecord.int64VectorFeature(pos_buffer.toArray)
    )
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
    val pos_buf = new ListBuffer[Long]
    pos_buf.append(0)

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
        // format: f_index:f_value__f_index:f_value
        var fmt: String = ""
        for (i <- 0 until rnfs.length) {
          bytebuf.putInt(shift, rnfs(i).f_index)
          fmt += rnfs(i).f_index.toString + ":"
          shift += 4
          bytebuf.putLong(shift, rnfs(i).feature_list(indexes(i)))
          fmt += rnfs(i).feature_list(indexes(i)).toString
          shift += 8
          fmt += "__"
        }
        fmt = fmt.stripSuffix("__")
        val p: LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(bytebuf.array(), 0, key_len, SEED, p)
        var hash = p.val1 % dim
        if (hash < 0) {
          hash += dim
        }
        if (pos_map.contains((f_index, hash))) {
          pos_buf.append(pos_map((f_index, hash)))
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
    builder.getFeaturesBuilder.putFeature(
      f_name + "_pos", TFRecord.int64VectorFeature(pos_buf.toArray)
    )
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
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ListBuffer[Long]]): Unit = {
  }

  /**
   * TSV
   *
   * @param dim     hash space dimension
   * @param pos_map (f_name, List[pos])
   */
  override def add(dim: Long, encoded_map: mutable.HashMap[String, ListBuffer[Long]], pos_map: immutable.HashMap[(Int, Long), Int]): Boolean = {
    true
  }
}

abstract class FeatureEncoder[T] {
  val raw_features: ListBuffer[RawFeature[T]] = new ListBuffer[RawFeature[T]]()
  val cross_features: ListBuffer[CrossFeature[T]] = new ListBuffer[CrossFeature[T]]()
  var target: Target[T] = _

  def setup(): FeatureEncoder[T]

  /**
   *
   * @return List[(f_name, f_index)]
   */
  def get_field_name_and_index(): ListBuffer[(String, Int)] = {
    val buff = new ListBuffer[(String, Int)]()
    for (raw_f <- raw_features) {
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
  def get_parquet_column_names(): ListBuffer[String] = {
    val buff = new ListBuffer[String]()
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
  def get_pos(input: T, dim: Long): ListBuffer[Long] = {
    val buf = ListBuffer[Long]()
    for (raw_f <- raw_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_features) {
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
   * @return List[(f_name, f_index, format, pos)]
   */
  def get_pos_info(input: T, dim: Long): ListBuffer[(String, Int, String, Long)] = {
    val buf = new ListBuffer[(String, Int, String, Long)]
    for (raw_f <- raw_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f <- raw_features) {
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
    for (raw_f <- raw_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    target.parse(input).add(builder)
    for (raw_f <- raw_features) {
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
    for (raw_f <- raw_features) {
      raw_f.clear()
      raw_f.parse(input)
    }

    val has_target = target
      .parse(input)
      .add(builder, target_map)

    var has_feature = false
    for (raw_f <- raw_features) {
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
    val encoded_map: mutable.HashMap[String, ListBuffer[Long]] = new mutable.HashMap[String, ListBuffer[Long]]()

    for (raw_f: RawFeature[T] <- raw_features) {
      raw_f.clear()
      raw_f.parse(input)
    }
    for (raw_f: RawFeature[T] <- raw_features) {
      raw_f.add(dim, encoded_map)
    }
    for (cross_f: CrossFeature[T] <- cross_features) {
      cross_f.add(dim, encoded_map)
    }

    val buff = new ListBuffer[(String, Long)]()
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
  def encode(input: T, dim: Long, pos_map: immutable.HashMap[(Int, Long), Int], target_map: immutable.HashMap[Int, Int]): (ParquetRecord, Boolean, Boolean) = {
    val builder = Example.newBuilder()
    val (has_feature, has_target) = encode(input, dim, builder, pos_map, target_map)
    (ParquetRecord.from_example(builder.build()), has_feature, has_target)
  }
}