package featurizer.core

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}
import utils.MurmurHash3
import utils.MurmurHash3.LongPair

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Cross feature encoder for feature interactions.
 *
 * Computes the Cartesian product of constituent categorical features and hashes
 * each combination into an embedding index. Supports second-order (2 features)
 * and third-order (3 features) crosses.
 *
 * The hash key concatenates all constituent `(f_index || feature_value)` pairs
 * in little-endian byte order, e.g. `1:action__xx__3:male` for a gender × genre cross.
 *
 * Each combination produces three TFRecord fields:
 *  - `{name}_raw`: human-readable combination string (e.g. "1:action__xx__3:male")
 *  - `{name}_index`: the hashed embedding position
 *  - `{name}_value`: 1.0 (constant weight for crosses)
 *
 * @tparam T the raw sample type
 * @param rnfs the constituent categorical features to cross
 */
class CrossFeature[T](f_i: Int, f_n: String, rnfs: CategoricalFeature[T]*) extends RawFeature(f_i, f_n, f_t = FeatureType.Categorical) {
  /** Current combination indices for iterating over the Cartesian product. */
  val indexes: Array[Int] = new Array[Int](rnfs.length)

  /** Byte length of the hash key: (4 + 8) × number of constituent features. */
  val key_len: Int = (4 + 8) * rnfs.length

  /** Computes MurmurHash3 over the concatenation of all constituent feature values. */
  def computeHash(dim: Long): Long = {
    if (dim <= 0) return 0L
    val bb = ByteBuffer.allocate(key_len).order(ByteOrder.LITTLE_ENDIAN)
    var shift = 0
    for (i <- 0 until rnfs.length) {
      bb.putInt(shift, rnfs(i).f_index)
      shift += 4
      bb.putLong(shift, rnfs(i).feature_list(indexes(i)))
      shift += 8
    }
    val p: LongPair = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(bb.array(), 0, key_len, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  /** Formats the current combination as a human-readable string (e.g. "1:action__xx__3:male"). */
  def formatCombination: String = {
    rnfs.indices.map(
      i => s"${rnfs(i).f_index}:${rnfs(i).raw_list(indexes(i))}"
    ).mkString("__xx__")
  }

  /** Iterates over the Cartesian product of all constituent feature values, skipping zero entries. */
  def foreachCombination(body: => Unit): Unit = {
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

  override def get_hash_info(dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    val buf = ArrayBuffer[(String, Int, Byte, String, Long, Float)]()
    foreachCombination {
      buf.append((f_name, f_index, f_type, formatCombination, computeHash(dim), 1.0F))
    }
    buf
  }

  /** Convenience overload: parses input, then computes hash info for all combinations. */
  def get_hash_info(input: T, dim: Long): ArrayBuffer[(String, Int, Byte, String, Long, Float)] = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    get_hash_info(dim)
  }

  override def get_hash(dim: Long): ArrayBuffer[Long] = {
    val buf = new ArrayBuffer[Long]
    foreachCombination {
      buf.append(computeHash(dim))
    }
    buf
  }

  /** Adds all combinations to a TF Example (no pos-map lookup). */
  def add(dim: Long, builder: Example.Builder): Unit = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    foreachCombination {
      raw_buf.append(formatCombination)
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

  /** Convenience overload: parses input, then adds all combinations to a TF Example. */
  def add(input: T, dim: Long, builder: Example.Builder): Unit = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    add(dim, builder)
  }

  /** Adds all combinations to a TF Example with pos-map lookup. Returns true if any combination survived filtering. */
  def add(dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)

    var has_feature = false
    foreachCombination {
      val fmt = formatCombination
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

  /** Convenience overload: parses input, then adds all combinations with pos-map lookup. */
  def add(input: T, dim: Long, builder: Example.Builder, pos_map: collection.Map[(Int, Long), Int]): Boolean = {
    for (c_f <- rnfs) {
      c_f.clear()
      c_f.parse(input)
    }
    add(dim, builder, pos_map)
  }

  /** Adds hashed positions to an encoded map (no pos-map lookup). */
  def add(dim: Long, encoded_map: mutable.HashMap[String, ArrayBuffer[Long]]): Unit = {
    foreachCombination {
      encoded_map.getOrElseUpdate(f_name, ArrayBuffer.empty[Long]).append(computeHash(dim))
    }
  }

  /** Adds positions to an encoded map with pos-map lookup. Returns true if any combination survived filtering. */
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

  /** Adds raw/position/value arrays to a Parquet columns map with pos-map lookup. Returns true if any combination survived filtering. */
  def add(dim: Long, pos_map: collection.Map[(Int, Long), Int], columns: mutable.Map[String, Any]): Boolean = {
    val raw_buf = new ArrayBuffer[String]()
    val pos_buf = new ArrayBuffer[Long]()
    val value_buf = new ArrayBuffer[Float]()
    raw_buf.append("R:")
    pos_buf.append(0L)
    value_buf.append(1.0F)
    foreachCombination {
      val fmt = formatCombination
      val hash = computeHash(dim)
      if (pos_map.contains((f_index, hash))) {
        raw_buf.append(fmt)
        pos_buf.append(pos_map((f_index, hash)).toLong)
        value_buf.append(1.0F)
      }
    }
    columns.put(f_name + "_raw", raw_buf)
    columns.put(f_name + "_index", pos_buf)
    columns.put(f_name + "_value", value_buf)
    pos_buf.length > 1
  }
}
