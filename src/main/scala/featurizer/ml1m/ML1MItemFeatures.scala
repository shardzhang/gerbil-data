package featurizer.ml1m

import featurizer.{CategoricalFeature, ContinuousFeature, RawFeature}
import utils.MurmurHash3
import utils.LogUtils.green_println

/**
 * ML-1M item features — movie title, genres, rating count, avg rating, hot rank, publish year
 */

/** Movie/item features: title tokens, genres, rating count, average rating, genre count, hot rank, publish year. */
class MovieID(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val movie_id = try {
      sample.item_id.toInt
    } catch {
      case e: Exception =>
        green_println(s"Featurizer4ML1M parse error: ${e.getMessage}")
        0
    }
    raw_list.append(sample.item_id)
    feature_list.append(movie_id)
    value_list.append(1.0F)
    this
  }
}

// fixme
class MovieTitle(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    try {
      val cleanTitle = sample.movie_title.replaceAll("\\s*\\(\\d+\\)\\s*$", "").trim
      val words = cleanTitle.split("\\s+")
      for (word <- words) {
        if (word.nonEmpty) {
          val p = new MurmurHash3.LongPair()
          MurmurHash3.murmurhash3_x64_128(word.getBytes, 0, word.length, SEED, p)
          raw_list.append(word)
          feature_list.append(p.val1)
          value_list.append(1.0F)
        }
      }
    } catch {
      case e: Exception =>
        green_println(s"Featurizer4ML1M parse error: ${e.getMessage}")
    }
    this
  }
}

class MovieGenres(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    for (gen <- sample.movie_genres) {
      if (gen != null && gen.nonEmpty) {
        val p = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes, 0, gen.length, SEED, p)
        raw_list.append(gen)
        feature_list.append(p.val1)
        value_list.append(1.0F)
      }
    }
    this
  }
}

class MovieRateCount(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val buck = sample.movie_rate_count match {
      case 0 => 1
      case x if x <= 2 => 2
      case x if x <= 5 => 3
      case x if x <= 10 => 4
      case x if x <= 20 => 5
      case x if x <= 50 => 6
      case x if x <= 100 => 7
      case x if x <= 200 => 8
      case x if x <= 500 => 9
      case x if x <= 1000 => 10
      case _ => 11
    }
    raw_list.append(sample.movie_rate_count.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class MovieAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val buck = sample.movie_avg_rate match {
      case x if x <= 0.0 => 1
      case x if x <= 1.0 => 2
      case x if x <= 2.0 => 3
      case x if x <= 2.5 => 4
      case x if x <= 3.0 => 5
      case x if x <= 3.5 => 6
      case x if x <= 4.0 => 7
      case x if x <= 4.5 => 8
      case _ => 9
    }
    raw_list.append(sample.movie_avg_rate.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class MovieAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val avg = try {
      sample.movie_avg_rate.toFloat
    } catch {
      case e: Exception =>
        green_println(s"Featurizer4ML1M parse error: ${e.getMessage}")
        0.0F
    }
    raw_list.append(avg.toString)
    feature_list.append(1L)
    value_list.append(avg)
    this
  }
}

class MovieGenreCnt(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val cnt = sample.movie_genres.size
    val buck = if (cnt >= 3) 3 else cnt
    raw_list.append(sample.movie_genres.size.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class MovieHotRank(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val rank = try {
      sample.movie_hot_rank
    } catch {
      case e: Exception =>
        green_println(s"Featurizer4ML1M parse error: ${e.getMessage}")
        99999
    }
    val buck = rank match {
      case x if x <= 100 => 4
      case x if x <= 500 => 3
      case x if x <= 2000 => 2
      case _ => 1
    }
    raw_list.append(rank.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class MoviePublishYear(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val buck = sample.movie_publish_year match {
      case x if x == 0 => 1
      case x if x < 1970 => 2
      case x if x < 1980 => 3
      case x if x < 1990 => 4
      case x if x < 2000 => 5
      case x if x < 2010 => 6
      case _ => 7
    }
    raw_list.append(sample.movie_publish_year.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}
