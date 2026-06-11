import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer

/**
 * Generates a small test pos_map.bin in the production binary format:
 *   - writeUTF field names (big-endian length prefix, per Java spec)
 *   - All other multi-byte fields: little-endian (via ByteBuffer)
 *
 * Build & run:
 *   cd /path/to/gerbil-data
 *   scalac -cp target/classes tools/DumpPosMapBin.scala -d tools/ && \
 *   scala -cp target/classes:tools DumpPosMapBin
 */
object DumpPosMapBin {

  val SEED: Int = 0x3c074a61
  val MAX_DIM: Long = 1L << 60

  val OUTPUT_PATH = "/tmp/test_pos_map.bin"

  def murmurHashString(s: String): Long = {
    val p = new utils.MurmurHash3.LongPair()
    val bytes = s.getBytes("UTF-8")
    utils.MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, SEED, p)
    p.val1
  }

  def computeHash(fIndex: Int, fea: Long): Long = {
    val bb = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(0, fIndex)
    bb.putLong(4, fea)
    val p = new utils.MurmurHash3.LongPair()
    utils.MurmurHash3.murmurhash3_x64_128(bb.array(), 0, 12, SEED, p)
    var hash = p.val1 % MAX_DIM
    if (hash < 0) hash += MAX_DIM
    hash
  }

  case class Entry(
    field_name: String,
    field_index: Int,
    field_type: Int,
    dim: Int,
    hash: Long,
    pos: Int,
    mean: Double,
    std: Double
  )

  /** Write a modified-UTF-8 string (matching Java DataOutputStream.writeUTF). */
  def writeUTF(buf: ByteBuffer, s: String): Unit = {
    val bytes = s.getBytes("UTF-8")
    require(bytes.length <= 65535, s"string too long: $s (${bytes.length} bytes)")
    // length in big-endian (Java DataOutputStream.writeUTF convention)
    buf.put((bytes.length >> 8).toByte)
    buf.put((bytes.length & 0xFF).toByte)
    buf.put(bytes)
  }

  def writeLEInt(buf: ByteBuffer, v: Int): Unit = buf.order(ByteOrder.LITTLE_ENDIAN).putInt(v)
  def writeLELong(buf: ByteBuffer, v: Long): Unit = buf.order(ByteOrder.LITTLE_ENDIAN).putLong(v)
  def writeLEDouble(buf: ByteBuffer, v: Double): Unit = buf.order(ByteOrder.LITTLE_ENDIAN).putDouble(v)

  def main(args: Array[String]): Unit = {
    val entries = ArrayBuffer[Entry]()

    // 1. user_age=25 → bucket 25, index=2
    entries += Entry("user_age", 2, 1, 8, computeHash(2, 25L), 0, 0.0, 1.0)

    // 2. user_gender=M → 1, index=3
    entries += Entry("user_gender", 3, 1, 4, computeHash(3, 1L), 0, 0.0, 1.0)

    // 3. user_occupation=4 → 4, index=4
    entries += Entry("user_occupation", 4, 1, 16, computeHash(4, 4L), 0, 0.0, 1.0)

    // 4. movie_genres=drama (string hash), index=103
    entries += Entry("movie_genres", 103, 1, 32, computeHash(103, murmurHashString("drama")), 0, 0.0, 1.0)

    // 5. movie_rate_count=200 → bucket 8, index=104
    entries += Entry("movie_rate_count", 104, 1, 16, computeHash(104, 8L), 0, 0.0, 1.0)

    // 6. movie_avg_rate_continue (continuous), index=109 → feature_id=1
    entries += Entry("movie_avg_rate_continue", 109, 0, 4, 1L, 0, 4.2, 1.5)

    // 7. context_time_hour=14 → 15, index=201
    entries += Entry("context_time_hour", 201, 1, 24, computeHash(201, 15L), 0, 0.0, 1.0)

    // 8. context_is_weekend=1 → 1, index=204
    entries += Entry("context_is_weekend", 204, 1, 4, computeHash(204, 1L), 0, 0.0, 1.0)

    // 9. movie_title token "One" hash, index=102
    entries += Entry("movie_title", 102, 1, 64, computeHash(102, murmurHashString("One")), 0, 0.0, 1.0)

    // 10. user_movie_rate item 1193, index=301
    entries += Entry("user_movie_rate", 301, 1, 128, computeHash(301, 1193L), 0, 0.0, 1.0)

    // ── Compute total size ──
    var totalSize = 8 + 4  // header: timestamp + pos_map_size
    for (e <- entries) {
      val nameBytes = e.field_name.getBytes("UTF-8").length
      totalSize += 2 + nameBytes  // writeUTF prefix + data
      totalSize += 4 + 4 + 4 + 8 + 4 + 8 + 8  // int32+int32+int32+int64+int32+double+double
    }
    totalSize += 4  // target_map_size
    totalSize += 2 * (4 + 4)  // 2 target entries (int32+int32 each)

    val buf = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN)

    // Section 1: Header
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(20260610L)    // timestamp (LE)
    buf.putInt(entries.size)  // pos_map_size (LE)

    // Section 2: Pos-map entries
    for (e <- entries) {
      writeUTF(buf, e.field_name)
      buf.order(ByteOrder.LITTLE_ENDIAN)
      buf.putInt(e.field_index)   // LE
      buf.putInt(e.field_type)    // LE
      buf.putInt(e.dim)           // LE
      buf.putLong(e.hash)         // LE
      buf.putInt(e.pos)           // LE
      buf.putDouble(e.mean)       // LE
      buf.putDouble(e.std)        // LE
    }

    // Section 3: Target-map
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(2)  // target_map_size
    buf.putInt(1193); buf.putInt(0)
    buf.putInt(1197); buf.putInt(1)

    // Write to file
    val fos = new FileOutputStream(OUTPUT_PATH)
    fos.write(buf.array())
    fos.close()

    println(s"Written ${entries.size} entries to $OUTPUT_PATH (${totalSize} bytes)")
    println("\nfield_name,field_index,field_type,dim,hash,pos,mean,std")
    for (e <- entries) {
      println(s"${e.field_name},${e.field_index},${e.field_type},${e.dim},${e.hash},${e.pos},${e.mean},${e.std}")
    }
    println("\ntarget_map: 1193→0, 1197→1")
  }
}
