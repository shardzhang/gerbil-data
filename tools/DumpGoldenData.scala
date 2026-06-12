import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

/**
 * Standalone golden data generator.
 * No external dependencies — just compile against the project's MurmurHash3.java.
 *
 * Build & run:
 *   cd /path/to/gerbil-data
 *   mvn package -q -DskipTests
 *   scalac -cp target/classes tools/DumpGoldenData.scala -d tools/ && \
 *   scala -cp target/classes:tools DumpGoldenData
 */
object DumpGoldenData {

  // ── constants ────────────────────────────────────────────────────────
  val SEED: Int    = 0x3c074a61
  val MAX_DIM: Long = 1L << 60

  val SAMPLE_RATING: Float = 5.0f

  // ── helpers ──────────────────────────────────────────────────────────

  def murmurHashString(s: String): Long = {
    val p = new utils.MurmurHash3.LongPair()
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    utils.MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, SEED, p)
    p.val1
  }

  def computeHash(fIndex: Int, fea: Long, dim: Long): Long = {
    val bb = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(0, fIndex)
    bb.putLong(4, fea)
    val p = new utils.MurmurHash3.LongPair()
    utils.MurmurHash3.murmurhash3_x64_128(bb.array(), 0, 12, SEED, p)
    var hash = p.val1 % dim
    if (hash < 0) hash += dim
    hash
  }

  // ── sample ───────────────────────────────────────────────────────────

  val SAMPLE: Sample = {
    val s = Sample()
    s.user_id = "1"; s.gender = "M"; s.age = "25"; s.occupation = "4"; s.zip_code = "12345"
    s.item_id = "1193"
    s.movie_title = "One Flew Over the Cuckoo's Nest (1975)"
    s.movie_publish_year = 1975
    s.movie_genres = ArrayBuffer("drama")
    s.movie_rate_count = 200; s.movie_avg_rate = 4.2; s.movie_hot_rank = 50; s.movie_genre_cnt = 1
    s.user_movie_rates       = ArrayBuffer((1193,5), (1197,3), (1198,4))
    s.user_movie_rate_1days  = ArrayBuffer((1193,5))
    s.user_movie_rate_3days  = ArrayBuffer((1193,5), (1197,3))
    s.user_movie_rate_7days  = ArrayBuffer((1193,5), (1197,3), (1198,4))
    s.user_movie_rate_15days = ArrayBuffer((1193,5), (1197,3), (1198,4))
    s.user_rate_cnt = 3; s.user_rate_7day_cnt = 3; s.user_rate_15day_cnt = 3; s.user_rate_30day_cnt = 3
    s.user_rate_std = 1.0f; s.user_rate_std_7day = 1.0f; s.user_rate_std_15day = 1.0f; s.user_rate_std_30day = 1.0f
    s.user_avg_rate = 4.0f; s.user_avg_rate_7day = 4.0f; s.user_avg_rate_15day = 4.0f; s.user_avg_rate_30day = 4.0f
    s.user_genres_rates       = ArrayBuffer(("drama",4.5f), ("comedy",3.5f))
    s.user_genres_rate_1days  = ArrayBuffer(("drama",4.5f))
    s.user_genres_rate_3days  = ArrayBuffer(("drama",4.5f), ("comedy",3.5f))
    s.user_genres_rate_7days  = ArrayBuffer(("drama",4.5f), ("comedy",3.5f))
    s.user_genres_rate_15days = ArrayBuffer(("drama",4.5f), ("comedy",3.5f))
    s.user_genres_rate_cnts       = ArrayBuffer(("drama",5), ("comedy",3))
    s.user_genres_rate_cnt_1days  = ArrayBuffer(("drama",2))
    s.user_genres_rate_cnt_3days  = ArrayBuffer(("drama",2), ("comedy",1))
    s.user_genres_rate_cnt_7days  = ArrayBuffer(("drama",4), ("comedy",2))
    s.user_genres_rate_cnt_15days = ArrayBuffer(("drama",5), ("comedy",3))
    s.user_top3_genres = ArrayBuffer(("drama",5), ("comedy",3), ("action",1))
    s.week_day = 3; s.time_hour = 14; s.time_area = 3; s.time_stamp = 1700000000000L
    s.target = 1193; s.rating = 5.0f; s.label = 1
    s
  }

  // ── feature definitions ──────────────────────────────────────────────

  case class FeatDef(name: String, index: Int, fType: Int) // 1=categorical, 0=continuous

  val FEATURES: Seq[FeatDef] = Seq(
    FeatDef("user_age", 2, 1), FeatDef("user_gender", 3, 1), FeatDef("user_occupation", 4, 1),
    FeatDef("user_rate_std", 6, 1), FeatDef("user_rate_std_7day", 7, 1), FeatDef("user_rate_std_15day", 8, 1), FeatDef("user_rate_std_30day", 9, 1),
    FeatDef("user_rate_std_continue", 18, 0), FeatDef("user_rate_std_7day_continue", 19, 0), FeatDef("user_rate_std_15day_continue", 21, 0), FeatDef("user_rate_std_30day_continue", 22, 0),
    FeatDef("user_movie_rate_cnt", 10, 1), FeatDef("user_movie_rate_cnt_7day", 11, 1), FeatDef("user_movie_rate_cnt_15day", 12, 1), FeatDef("user_movie_rate_cnt_30day", 13, 1),
    FeatDef("user_avg_rate", 14, 1), FeatDef("user_avg_rate_7day", 15, 1), FeatDef("user_avg_rate_15day", 16, 1), FeatDef("user_avg_rate_30day", 17, 1),
    FeatDef("user_avg_rate_continue", 23, 0), FeatDef("user_avg_rate_7day_continue", 24, 0), FeatDef("user_avg_rate_15day_continue", 25, 0), FeatDef("user_avg_rate_30day_continue", 26, 0),
    FeatDef("movie_title", 102, 1), FeatDef("movie_genres", 103, 1), FeatDef("movie_rate_count", 104, 1), FeatDef("movie_avg_rate", 105, 1),
    FeatDef("movie_genre_cnt", 106, 1), FeatDef("item_hot_rank", 107, 1), FeatDef("movie_publish_year", 108, 1), FeatDef("movie_avg_rate_continue", 109, 0),
    FeatDef("context_time_hour", 201, 1), FeatDef("context_time_area", 202, 1), FeatDef("context_time_week", 203, 1), FeatDef("context_is_weekend", 204, 1),
    FeatDef("user_movie_rate", 301, 1), FeatDef("user_movie_rate_1day", 302, 1), FeatDef("user_movie_rate_3day", 303, 1), FeatDef("user_movie_rate_7day", 304, 1), FeatDef("user_movie_rate_15day", 305, 1),
    FeatDef("user_genres_rate", 306, 1), FeatDef("user_genres_rate_1day", 307, 1), FeatDef("user_genres_rate_3day", 308, 1), FeatDef("user_genres_rate_7day", 309, 1), FeatDef("user_genres_rate_15day", 310, 1),
    FeatDef("user_genres_rate_cnts", 312, 1), FeatDef("user_genres_rate_cnt_1days", 313, 1), FeatDef("user_genres_rate_cnt_3days", 314, 1), FeatDef("user_genres_rate_cnt_7days", 315, 1), FeatDef("user_genres_rate_cnt_15days", 316, 1),
    FeatDef("user_top3_genres", 317, 1),
    FeatDef("user_watch_same_genre", 351, 1), FeatDef("user_watch_same_genre_1day", 352, 1), FeatDef("user_watch_same_genre_3day", 353, 1), FeatDef("user_watch_same_genre_7day", 354, 1), FeatDef("user_watch_same_genre_15day", 355, 1),
    FeatDef("user_same_genre_avg_rate", 356, 1), FeatDef("user_same_genre_avg_rate_continue", 357, 0)
  )

  // ── cross feature definitions ────────────────────────────────────────

  case class CrossFeatDef(name: String, index: Int, depends: Seq[String])

  val CROSS_FEATURES: Seq[CrossFeatDef] = Seq(
    CrossFeatDef("movie_genres_xx_user_genres_rate", 401, Seq("movie_genres", "user_genres_rate")),
    CrossFeatDef("movie_genres_xx_user_genres_rate_1day", 402, Seq("movie_genres", "user_genres_rate_1day")),
    CrossFeatDef("movie_publish_year_xx_user_age", 406, Seq("movie_publish_year", "user_age")),
    CrossFeatDef("movie_rate_count_xx_user_rate_std", 410, Seq("movie_rate_count", "user_rate_std")),
    CrossFeatDef("movie_hot_rank_xx_user_avg_rate", 411, Seq("item_hot_rank", "user_avg_rate")),
    CrossFeatDef("movie_publish_year_xx_user_avg_rate", 412, Seq("movie_publish_year", "user_avg_rate")),
    CrossFeatDef("movie_genre_cnt_xx_user_avg_rate", 413, Seq("movie_genre_cnt", "user_avg_rate")),
    CrossFeatDef("movie_hot_rank_xx_user_genre_avg_rate", 414, Seq("item_hot_rank", "user_same_genre_avg_rate")),
    CrossFeatDef("movie_genres_xx_user_gender", 417, Seq("movie_genres", "user_gender")),
    CrossFeatDef("movie_genres_xx_user_occupation", 418, Seq("movie_genres", "user_occupation")),
    CrossFeatDef("movie_genres_xx_user_age", 419, Seq("movie_genres", "user_age")),
    CrossFeatDef("movie_genres_xx_is_weekend", 450, Seq("movie_genres", "context_is_weekend")),
    CrossFeatDef("movie_hot_rank_xx_is_weekend", 451, Seq("item_hot_rank", "context_is_weekend")),
    CrossFeatDef("user_age_xx_is_weekend", 452, Seq("user_age", "context_is_weekend")),
    CrossFeatDef("user_gender_xx_context_time_hour", 453, Seq("user_gender", "context_time_hour")),
    CrossFeatDef("movie_genres_xx_user_age_xx_user_gender", 460, Seq("movie_genres", "user_age", "user_gender")),
    CrossFeatDef("movie_genres_xx_user_gender_xx_user_occupation", 461, Seq("movie_genres", "user_gender", "user_occupation")),
    CrossFeatDef("movie_publish_year_xx_user_age_xx_user_occupation", 462, Seq("movie_publish_year", "user_age", "user_occupation")),
    CrossFeatDef("movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate", 463, Seq("movie_avg_rate", "item_hot_rank", "user_avg_rate")),
    CrossFeatDef("movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate", 465, Seq("movie_genre_cnt", "user_movie_rate_cnt", "user_avg_rate")),
    CrossFeatDef("movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate", 466, Seq("movie_genre_cnt", "item_hot_rank", "user_same_genre_avg_rate")),
    CrossFeatDef("movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate", 467, Seq("movie_publish_year", "movie_avg_rate", "user_avg_rate"))
  )

  // ── cross feature encoding ───────────────────────────────────────────

  def encodeCross(s: Sample, cfd: CrossFeatDef, fdMap: Map[String, FeatDef]): Seq[EncodedVal] = {
    // Get constituent feature values using the same encode() function
    val constituents: Seq[(Int, Seq[EncodedVal])] = cfd.depends.map { name =>
      val fd = fdMap(name)
      (fd.index, encode(s, fd))
    }
    if (constituents.isEmpty || constituents.exists(_._2.isEmpty)) return Seq.empty

    val buf = ArrayBuffer.empty[EncodedVal]

    def combine(depth: Int, current: Seq[(Int, String, Long)]): Unit = {
      if (depth == constituents.length) {
        val keyLen = (4 + 8) * current.size
        val bb = ByteBuffer.allocate(keyLen).order(ByteOrder.LITTLE_ENDIAN)
        var off = 0
        for ((fIdx, _, fea) <- current) {
          bb.putInt(off, fIdx); off += 4
          bb.putLong(off, fea); off += 8
        }
        val p = new utils.MurmurHash3.LongPair()
        utils.MurmurHash3.murmurhash3_x64_128(bb.array(), 0, keyLen, SEED, p)
        var hash = p.val1 % MAX_DIM
        if (hash < 0) hash += MAX_DIM
        val combStr = current.map { case (fIdx, raw, _) => s"$fIdx:$raw" }.mkString("__xx__")
        buf += EncodedVal(combStr, p.val1, hash, 1.0f)
      } else {
        for (ev <- constituents(depth)._2) {
          combine(depth + 1, current :+ (constituents(depth)._1, ev.raw, ev.featureId))
        }
      }
    }

    combine(0, Seq.empty)
    buf.toSeq
  }

  // ── feature logic ────────────────────────────────────────────────────

  case class EncodedVal(raw: String, featureId: Long, hash: Long, value: Float)

  def stripYear(title: String): String = {
    var pos = title.length
    while (pos > 0 && (title(pos - 1) == ' ' || title(pos - 1) == '\t')) pos -= 1
    if (pos == 0 || title(pos - 1) != ')') return title
    pos -= 1
    val endDigits = pos
    while (pos > 0 && title(pos - 1) >= '0' && title(pos - 1) <= '9') pos -= 1
    if (endDigits == pos) return title
    if (pos == 0 || title(pos - 1) != '(') return title
    pos -= 1
    while (pos > 0 && (title(pos - 1) == ' ' || title(pos - 1) == '\t')) pos -= 1
    title.substring(0, pos)
  }

  def encode(s: Sample, fd: FeatDef): Seq[EncodedVal] = {
    val buf = ArrayBuffer.empty[EncodedVal]
    fd match {
      // ── User demographics ──
      case FeatDef("user_age", idx, _) =>
        val fea = try { s.age.toInt } catch { case _: Exception => 0 }
        buf += EncodedVal(s.age, fea, computeHash(idx, fea, MAX_DIM), 1.0f)

      case FeatDef("user_gender", idx, _) =>
        val fea = s.gender match { case "M" => 1; case "F" => 2; case _ => 0 }
        buf += EncodedVal(s.gender, fea, computeHash(idx, fea, MAX_DIM), 1.0f)

      case FeatDef("user_occupation", idx, _) =>
        val fea = try { s.occupation.toInt } catch { case _: Exception => 0 }
        buf += EncodedVal(s.occupation, fea, computeHash(idx, fea, MAX_DIM), 1.0f)

      // ── User rate std (bucketed) ──
      case FeatDef(n, idx, _) if n.startsWith("user_rate_std") && !n.contains("continue") =>
        val std = n match {
          case "user_rate_std"          => s.user_rate_std
          case "user_rate_std_7day"     => s.user_rate_std_7day
          case "user_rate_std_15day"    => s.user_rate_std_15day
          case "user_rate_std_30day"    => s.user_rate_std_30day
        }
        val buck: Long = if (std <= 0.0f) 1 else if (std <= 1.0f) 2 else if (std <= 2.0f) 3 else 4
        buf += EncodedVal(std.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      // ── User rate std (continuous) ──
      case FeatDef(n, idx, 0) if n.startsWith("user_rate_std") =>
        val std = n match {
          case "user_rate_std_continue"          => s.user_rate_std
          case "user_rate_std_7day_continue"     => s.user_rate_std_7day
          case "user_rate_std_15day_continue"    => s.user_rate_std_15day
          case "user_rate_std_30day_continue"    => s.user_rate_std_30day
        }
        buf += EncodedVal(std.toString, 1L, 1L, std)

      // ── User movie rate cnt (bucketed) ──
      case FeatDef("user_movie_rate_cnt", idx, _) =>
        val buck = s.user_rate_cnt match { case x if x <= 10 => 1; case x if x <= 30 => 2; case x if x <= 50 => 3; case x if x <= 100 => 4; case _ => 5 }
        buf += EncodedVal(s.user_rate_cnt.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)
      case FeatDef("user_movie_rate_cnt_7day", idx, _) =>
        val buck = s.user_rate_7day_cnt match { case x if x <= 10 => 1; case x if x <= 30 => 2; case x if x <= 50 => 3; case x if x <= 100 => 4; case _ => 5 }
        buf += EncodedVal(s.user_rate_7day_cnt.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)
      case FeatDef("user_movie_rate_cnt_15day", idx, _) =>
        val buck = s.user_rate_15day_cnt match { case 0 => 1; case x if x <= 10 => 2; case x if x <= 30 => 3; case x if x <= 50 => 4; case x if x <= 100 => 5; case _ => 6 }
        buf += EncodedVal(s.user_rate_15day_cnt.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)
      case FeatDef("user_movie_rate_cnt_30day", idx, _) =>
        val buck = s.user_rate_30day_cnt match { case 0 => 1; case x if x <= 10 => 2; case x if x <= 30 => 3; case x if x <= 50 => 4; case x if x <= 100 => 5; case _ => 6 }
        buf += EncodedVal(s.user_rate_30day_cnt.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      // ── User avg rate (bucketed) ──
      case FeatDef(n, idx, _) if n.startsWith("user_avg_rate") && !n.contains("continue") =>
        val avg = n match {
          case "user_avg_rate"       => s.user_avg_rate
          case "user_avg_rate_7day"  => s.user_avg_rate_7day
          case "user_avg_rate_15day" => s.user_avg_rate_15day
          case "user_avg_rate_30day" => s.user_avg_rate_30day
        }
        val buck: Long = if (avg == 0.0f) 1 else if (avg < 3.0f) 2 else if (avg < 4.0f) 3 else 4
        buf += EncodedVal(avg.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      // ── User avg rate (continuous) ──
      case FeatDef(n, idx, 0) if n.startsWith("user_avg_rate") =>
        val avg = n match {
          case "user_avg_rate_continue"          => s.user_avg_rate
          case "user_avg_rate_7day_continue"     => s.user_avg_rate_7day
          case "user_avg_rate_15day_continue"    => s.user_avg_rate_15day
          case "user_avg_rate_30day_continue"    => s.user_avg_rate_30day
        }
        buf += EncodedVal(avg.toString, 1L, 1L, avg)

      // ── Item features ──
      case FeatDef("movie_title", idx, _) =>
        val clean = stripYear(s.movie_title).trim
        for (w <- clean.split("\\s+") if w.nonEmpty) {
          val id = murmurHashString(w)
          buf += EncodedVal(w, id, computeHash(idx, id, MAX_DIM), 1.0f)
        }

      case FeatDef("movie_genres", idx, _) =>
        for (g <- s.movie_genres if g.nonEmpty) {
          val id = murmurHashString(g)
          buf += EncodedVal(g, id, computeHash(idx, id, MAX_DIM), 1.0f)
        }

      case FeatDef("movie_rate_count", idx, _) =>
        val buck: Long = s.movie_rate_count match {
          case 0 => 1; case x if x <= 2 => 2; case x if x <= 5 => 3; case x if x <= 10 => 4
          case x if x <= 20 => 5; case x if x <= 50 => 6; case x if x <= 100 => 7
          case x if x <= 200 => 8; case x if x <= 500 => 9; case x if x <= 1000 => 10; case _ => 11
        }
        buf += EncodedVal(s.movie_rate_count.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      case FeatDef("movie_avg_rate", idx, _) =>
        val buck: Long = s.movie_avg_rate match {
          case x if x <= 0.0 => 1; case x if x <= 1.0 => 2; case x if x <= 2.0 => 3; case x if x <= 2.5 => 4
          case x if x <= 3.0 => 5; case x if x <= 3.5 => 6; case x if x <= 4.0 => 7; case x if x <= 4.5 => 8; case _ => 9
        }
        buf += EncodedVal(s.movie_avg_rate.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      case FeatDef("movie_genre_cnt", idx, _) =>
        val buck: Long = if (s.movie_genre_cnt >= 3) 3 else s.movie_genre_cnt
        buf += EncodedVal(s.movie_genre_cnt.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      case FeatDef("item_hot_rank", idx, _) =>
        val buck: Long = s.movie_hot_rank match {
          case x if x <= 100 => 4; case x if x <= 500 => 3; case x if x <= 2000 => 2; case _ => 1
        }
        buf += EncodedVal(s.movie_hot_rank.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      case FeatDef("movie_publish_year", idx, _) =>
        val buck: Long = s.movie_publish_year match {
          case 0 => 1; case x if x < 1970 => 2; case x if x < 1980 => 3; case x if x < 1990 => 4
          case x if x < 2000 => 5; case x if x < 2010 => 6; case _ => 7
        }
        buf += EncodedVal(s.movie_publish_year.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      case FeatDef("movie_avg_rate_continue", idx, 0) =>
        val v = s.movie_avg_rate.toFloat
        buf += EncodedVal(v.toString, 1L, 1L, v)

      // ── Context ──
      case FeatDef("context_time_hour", idx, _) =>
        val fea = s.time_hour + 1
        buf += EncodedVal(s.time_hour.toString, fea, computeHash(idx, fea, MAX_DIM), 1.0f)
      case FeatDef("context_time_area", idx, _) =>
        val fea = s.time_area + 1
        buf += EncodedVal(s.time_area.toString, fea, computeHash(idx, fea, MAX_DIM), 1.0f)
      case FeatDef("context_time_week", idx, _) =>
        val fea = s.week_day
        buf += EncodedVal(s.week_day.toString, fea, computeHash(idx, fea, MAX_DIM), 1.0f)
      case FeatDef("context_is_weekend", idx, _) =>
        val fea = if (s.week_day == 6 || s.week_day == 7) 2 else 1
        buf += EncodedVal(fea.toString, fea, computeHash(idx, fea, MAX_DIM), 1.0f)

      // ── User movie rate sequences ──
      case FeatDef(n, idx, _) if n.startsWith("user_movie_rate") && !n.contains("genres") =>
        val rates = n match {
          case "user_movie_rate"      => s.user_movie_rates
          case "user_movie_rate_1day" => s.user_movie_rate_1days
          case "user_movie_rate_3day" => s.user_movie_rate_3days
          case "user_movie_rate_7day" => s.user_movie_rate_7days
          case "user_movie_rate_15day"=> s.user_movie_rate_15days
        }
        for (i <- 0 until math.min(200, rates.size)) {
          val (mid, rate) = rates(i)
          buf += EncodedVal(mid.toString, mid, computeHash(idx, mid, MAX_DIM), rate.toFloat)
        }

      // ── User genre rate sequences ──
      case FeatDef(n, idx, _) if n.startsWith("user_genres_rate") && !n.contains("cnt") =>
        val rates = n match {
          case "user_genres_rate"       => s.user_genres_rates
          case "user_genres_rate_1day"  => s.user_genres_rate_1days
          case "user_genres_rate_3day"  => s.user_genres_rate_3days
          case "user_genres_rate_7day"  => s.user_genres_rate_7days
          case "user_genres_rate_15day" => s.user_genres_rate_15days
        }
        for (i <- 0 until math.min(200, rates.size)) {
          val (gen, avg) = rates(i)
          val id = murmurHashString(gen)
          buf += EncodedVal(gen, id, computeHash(idx, id, MAX_DIM), avg)
        }

      // ── User genre rate counts ──
      case FeatDef(n, idx, _) if n.startsWith("user_genres_rate_cnt") =>
        val cnts = n match {
          case "user_genres_rate_cnts"         => s.user_genres_rate_cnts
          case "user_genres_rate_cnt_1days"    => s.user_genres_rate_cnt_1days
          case "user_genres_rate_cnt_3days"    => s.user_genres_rate_cnt_3days
          case "user_genres_rate_cnt_7days"    => s.user_genres_rate_cnt_7days
          case "user_genres_rate_cnt_15days"   => s.user_genres_rate_cnt_15days
        }
        for (i <- 0 until math.min(200, cnts.size)) {
          val gen = cnts(i)._1.trim.toLowerCase
          val cnt = cnts(i)._2
          val id = murmurHashString(gen)
          buf += EncodedVal(gen, id, computeHash(idx, id, MAX_DIM), cnt.toFloat)
        }

      case FeatDef("user_top3_genres", idx, _) =>
        for ((g, cnt) <- s.user_top3_genres if g.nonEmpty) {
          val id = murmurHashString(g)
          buf += EncodedVal(g, id, computeHash(idx, id, MAX_DIM), cnt.toFloat)
        }

      // ── User watch same genre ──
      case FeatDef("user_watch_same_genre", idx, _) =>
        val cur = s.movie_genres.toSet
        val rec = s.user_genres_rates.map(_._1).toSet
        val flag: Long = if (cur.isEmpty || rec.isEmpty) 1 else if (cur.intersect(rec).nonEmpty) 2 else 1
        buf += EncodedVal(flag.toString, flag, computeHash(idx, flag, MAX_DIM), 1.0f)

      case FeatDef("user_watch_same_genre_1day", idx, _) =>
        val cur = s.movie_genres.toSet
        val rec = s.user_genres_rate_1days.map(_._1).toSet
        val flag: Long = if (cur.intersect(rec).nonEmpty) 2 else 1
        buf += EncodedVal(flag.toString, flag, computeHash(idx, flag, MAX_DIM), 1.0f)

      case FeatDef("user_watch_same_genre_3day", idx, _) =>
        val cur = s.movie_genres.toSet
        val rec = s.user_genres_rate_3days.map(_._1).toSet
        val flag: Long = if (cur.intersect(rec).nonEmpty) 2 else 1
        buf += EncodedVal(flag.toString, flag, computeHash(idx, flag, MAX_DIM), 1.0f)

      case FeatDef("user_watch_same_genre_7day", idx, _) =>
        val cur = s.movie_genres.toSet
        val rec = s.user_genres_rate_7days.map(_._1).toSet
        val flag: Long = if (cur.intersect(rec).nonEmpty) 2 else 1
        buf += EncodedVal(flag.toString, flag, computeHash(idx, flag, MAX_DIM), 1.0f)

      case FeatDef("user_watch_same_genre_15day", idx, _) =>
        val cur = s.movie_genres.toSet
        val rec = s.user_genres_rate_15days.map(_._1).toSet
        val flag: Long = if (cur.intersect(rec).nonEmpty) 2 else 1
        buf += EncodedVal(flag.toString, flag, computeHash(idx, flag, MAX_DIM), 1.0f)

      // ── User same genre avg rate ──
      case FeatDef("user_same_genre_avg_rate", idx, _) =>
        val genreMap = s.user_genres_rates.toMap
        val rates = s.movie_genres.flatMap(g => genreMap.get(g))
        val finalRate = if (rates.isEmpty) 3.0f else rates.sum / rates.size
        val buck: Long = if (finalRate <= 1.0f) 1 else if (finalRate <= 2.0f) 2 else if (finalRate <= 3.0f) 3 else if (finalRate <= 4.0f) 4 else 5
        buf += EncodedVal(finalRate.toString, buck, computeHash(idx, buck, MAX_DIM), 1.0f)

      case FeatDef("user_same_genre_avg_rate_continue", idx, 0) =>
        val genreMap = s.user_genres_rates.toMap
        val rates = s.movie_genres.flatMap(g => genreMap.get(g))
        val finalRate = if (rates.isEmpty) 3.0f else rates.sum / rates.size
        buf += EncodedVal(finalRate.toString, 1L, 1L, finalRate)

      case _ => // should not happen
    }
    buf.toSeq
  }

  // ── main ─────────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    println("feature_name,field_index,field_type,raw,feature_id,hash,value")
    val fdMap = FEATURES.map(fd => fd.name -> fd).toMap
    for (fd <- FEATURES) {
      val vals = encode(SAMPLE, fd)
      for (v <- vals) {
        println(s"${fd.name},${fd.index},${fd.fType},${v.raw},${v.featureId},${v.hash},${v.value}")
      }
    }
    for (cfd <- CROSS_FEATURES) {
      val vals = encodeCross(SAMPLE, cfd, fdMap)
      for (v <- vals) {
        println(s"${cfd.name},${cfd.index},1,${v.raw},${v.featureId},${v.hash},${v.value}")
      }
    }
  }
}

// ── data class ────────────────────────────────────────────────────────

case class Sample(
  var user_id: String = "",
  var gender: String = "",
  var age: String = "",
  var occupation: String = "",
  var zip_code: String = "",
  var item_id: String = "",
  var movie_title: String = "",
  var movie_publish_year: Int = 0,
  var movie_genres: ArrayBuffer[String] = ArrayBuffer.empty,
  var movie_rate_count: Long = 0,
  var movie_avg_rate: Double = 0.0,
  var movie_hot_rank: Int = 99999,
  var movie_genre_cnt: Int = 0,
  var user_movie_rates: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty,
  var user_movie_rate_1days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty,
  var user_movie_rate_3days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty,
  var user_movie_rate_7days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty,
  var user_movie_rate_15days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty,
  var user_rate_cnt: Int = 0,
  var user_rate_7day_cnt: Int = 0,
  var user_rate_15day_cnt: Int = 0,
  var user_rate_30day_cnt: Int = 0,
  var user_rate_std: Float = 0.0f,
  var user_rate_std_7day: Float = 0.0f,
  var user_rate_std_15day: Float = 0.0f,
  var user_rate_std_30day: Float = 0.0f,
  var user_avg_rate: Float = 3.0f,
  var user_avg_rate_7day: Float = 3.0f,
  var user_avg_rate_15day: Float = 3.0f,
  var user_avg_rate_30day: Float = 3.0f,
  var user_genres_rates: ArrayBuffer[(String, Float)] = ArrayBuffer.empty,
  var user_genres_rate_1days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty,
  var user_genres_rate_3days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty,
  var user_genres_rate_7days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty,
  var user_genres_rate_15days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty,
  var user_genres_rate_cnts: ArrayBuffer[(String, Int)] = ArrayBuffer.empty,
  var user_genres_rate_cnt_1days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty,
  var user_genres_rate_cnt_3days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty,
  var user_genres_rate_cnt_7days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty,
  var user_genres_rate_cnt_15days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty,
  var user_top3_genres: ArrayBuffer[(String, Int)] = ArrayBuffer.empty,
  var week_day: Int = 0,
  var time_hour: Int = 0,
  var time_area: Int = 0,
  var time_stamp: Long = 0L,
  var target: Int = 0,
  var label: Int = 0,
  var rating: Float = 0.0f
)
