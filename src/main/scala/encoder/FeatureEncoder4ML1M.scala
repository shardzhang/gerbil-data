package encoder

import vectorizer.{CrossFeature, FeatureEncoder, FeatureType, RawFeature, RawTarget, CategoricalFeature, ContinuousFeature}
import sample.ML1MTrainSample
import utils.MurmurHash3
import utils.LogUtils.green_println

/**
 * Feature encoder for the ML1M dataset.
 *
 * Single-value: Int/Long kept as-is; Float/Double bucketized; String hashed.
 * Multi-value: each element processed individually; strings hashed via MurmurHash3.
 */
class FeatureEncoder4ML1M extends FeatureEncoder[ML1MTrainSample] {
  private type T = ML1MTrainSample

  // ============================== target ==============================

  /** Multi-class classification target. */
  private class Target extends RawTarget[T] {
    override def parse(sample: T): RawTarget[T] = {
      target = sample.target
      this
    }
  }

  /** Binary classification label (rating >= 3 => positive). */
  private class Label extends RawTarget[T] {
    override def parse(sample: T): RawTarget[T] = {
      target = if (sample.rating >= 3) {
        1.0F
      } else {
        0.0F
      }
      this
    }
  }

  /** Regression target: raw rating value. */
  private class Rating extends RawTarget[T] {
    override def parse(sample: T): RawTarget[T] = {
      target = sample.rating
      this
    }
  }

  // ============================== item ==============================

  /** Movie ID as categorical feature. */
  private class MovieID(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val movie_id = try {
        sample.item_id.toInt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      raw_list.append(sample.item_id)
      feature_list.append(movie_id)
      value_list.append(1.0F)
      this
    }
  }

  /** Movie title as multi-value feature (words hashed). Mostly redundant with movie_id. */
  private class MovieTitle(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
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
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          }
      this
    }
  }

  /** Movie genres as multi-value categorical feature. */
  private class MovieGenres(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (gen <- sample.movie_genres) {
        if (gen != null && gen.nonEmpty) {
          val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
          MurmurHash3.murmurhash3_x64_128(gen.getBytes, 0, gen.length, SEED, p)
          raw_list.append(gen)
          feature_list.append(p.val1)
          value_list.append(1.0F)
        }
      }
      this
    }
  }

  /** Movie review count, bucketized into 11 tiers. */
  private class MovieRateCount(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
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

  /** Movie average rating, bucketized into 9 tiers. */
  private class MovieAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
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

  private class MovieAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val avg = try {
        sample.movie_avg_rate.toFloat
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0F
      }
      raw_list.append(avg.toString)
      feature_list.append(1L)
      value_list.append(avg)
      this
    }
  }

  /** Number of genres a movie belongs to (1/2/3+), bucketized. More genres = wider appeal. */
  private class MovieGenreCnt(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val cnt = sample.movie_genres.size
      // 1/2/3+
      val buck = if (cnt >= 3) 3 else cnt
      raw_list.append(sample.movie_genres.size.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  /** Movie popularity rank bucketized: blockbuster / popular / average / long-tail (cold-start). */
  private class MovieHotRank(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val rank = try {
        sample.movie_hot_rank
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          99999
      }
      val buck = rank match {
        case x if x <= 100 => 4 // blockbuster
        case x if x <= 500 => 3 // popular
        case x if x <= 2000 => 2 // average
        case _ => 1 // long-tail
      }
      raw_list.append(rank.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  /** Movie publish year extracted from title, bucketized by decade. */
  private class MoviePublishYear(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
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

  // ============================== user ==============================

  /** User ID as categorical feature. */
  private class UserID(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val user_id = try {
        sample.user_id.toInt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      raw_list.append(sample.user_id.toString)
      feature_list.append(user_id)
      value_list.append(1.0F)
      this
    }
  }

  /** User age as categorical feature. */
  private class UserAge(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val age = try {
        sample.age.toInt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      raw_list.append(sample.age.toString)
      feature_list.append(age)
      value_list.append(1.0F)
      this
    }
  }

  /** User gender: M=1, F=2. */
  private class UserGender(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val buck = sample.gender match {
        case "M" => 1
        case "F" => 2
        case _ => 0
      }
      raw_list.append(sample.gender.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  /** User occupation as categorical feature. */
  private class UserOccupation(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val occupation = try {
        sample.occupation.toInt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      raw_list.append(sample.occupation.toString)
      feature_list.append(occupation)
      value_list.append(1.0F)
      this
    }
  }

  /** User zip code, hashed via MurmurHash3. */
  private class UserZipCode(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val hash = try {
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(sample.zip_code.getBytes(), 0, sample.zip_code.length, SEED, p)
        p.val1
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0L
      }
      raw_list.append(sample.zip_code.toString)
      feature_list.append(hash)
      value_list.append(1.0F)
      this
    }
  }

  /** User rating std-dev bucketized: picky (high variance) vs easygoing (low variance). */
  private class UserRateStd(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0
      }
      // buckets: 1=identical, 2=stable, 3=moderate, 4=picky
      val buck = std match {
        case x if x <= 0.0 => 1
        case x if x <= 1.0 => 2
        case x if x <= 2.0 => 3
        case _ => 4
      }
      raw_list.append(sample.user_rate_std.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserRateStdContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0F
      }
      raw_list.append(std.toString)
      feature_list.append(1L)
      value_list.append(std)
      this
    }
  }

  private class UserRateStd7Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std_7day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0
      }
      // buckets: 1=identical, 2=stable, 3=moderate, 4=picky
      val buck = std match {
        case x if x <= 0.0 => 1
        case x if x <= 1.0 => 2
        case x if x <= 2.0 => 3
        case _ => 4
      }
      raw_list.append(sample.user_rate_std_7day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserRateStd7DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std_7day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0F
      }
      raw_list.append(std.toString)
      feature_list.append(1L)
      value_list.append(std)
      this
    }
  }

  private class UserRateStd15Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std_15day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0
      }
      // buckets: 1=identical, 2=stable, 3=moderate, 4=picky
      val buck = std match {
        case x if x <= 0.0 => 1
        case x if x <= 1.0 => 2
        case x if x <= 2.0 => 3
        case _ => 4
      }
      raw_list.append(sample.user_rate_std_15day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserRateStd15DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std_15day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0F
      }
      raw_list.append(std.toString)
      feature_list.append(1L)
      value_list.append(std)
      this
    }
  }

  private class UserRateStd30Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std_30day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0
      }
      // buckets: 1=identical, 2=stable, 3=moderate, 4=picky
      val buck = std match {
        case x if x <= 0.0 => 1
        case x if x <= 1.0 => 2
        case x if x <= 2.0 => 3
        case _ => 4
      }
      raw_list.append(sample.user_rate_std_30day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserRateStd30DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val std = try {
        sample.user_rate_std_30day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0.0F
      }
      raw_list.append(std.toString)
      feature_list.append(1L)
      value_list.append(std)
      this
    }
  }

  /** User active days bucketized: new / regular / power user. */
  private class UserActiveDay(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val days = try {
        sample.user_active_day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      val buck = days match {
        case 0 => 1
        case x if x <= 7 => 2 // within 1 week
        case x if x <= 30 => 3 // within 1 month
        case _ => 4 // veteran
      }
      raw_list.append(sample.user_active_day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  /** Total user rating count (behavior richness). */
  private class UserMovieRateCnt(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val cnt = try {
        sample.user_rate_cnt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      // buckets: user rating count ranges
      val buck = cnt match {
        case x if x <= 10 => 1
        case x if x <= 30 => 2
        case x if x <= 50 => 3
        case x if x <= 100 => 4
        case _ => 5
      }
      raw_list.append(sample.user_rate_cnt.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserMovieRateCnt7Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val cnt = try {
        sample.user_rate_7day_cnt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      // buckets: user rating count ranges
      val buck = cnt match {
        case x if x <= 10 => 1
        case x if x <= 30 => 2
        case x if x <= 50 => 3
        case x if x <= 100 => 4
        case _ => 5
      }
      raw_list.append(sample.user_rate_7day_cnt.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserMovieRateCnt15Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val cnt = try {
        sample.user_rate_15day_cnt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      // buckets: user rating count ranges
      val buck = cnt match {
        case x if x == 0 => 1
        case x if x <= 10 => 2
        case x if x <= 30 => 3
        case x if x <= 50 => 4
        case x if x <= 100 => 5
        case _ => 6
      }
      raw_list.append(sample.user_rate_15day_cnt.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserMovieRateCnt30Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val cnt = try {
        sample.user_rate_30day_cnt
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      // buckets: user rating count ranges
      val buck = cnt match {
        case x if x == 0 => 1
        case x if x <= 10 => 2
        case x if x <= 30 => 3
        case x if x <= 50 => 4
        case x if x <= 100 => 5
        case _ => 6
      }
      raw_list.append(sample.user_rate_30day_cnt.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  /** User historical average rating: low (<3) / medium (3-4) / high (>=4). */
  private class UserAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val avg = try {
        sample.user_avg_rate
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0
      }
      // low (<3), medium (3-4), high (>=4)
      val buck = avg match {
        case x if x == 0.0 => 1
        case x if x < 3.0 => 2 // low-rating user
        case x if x < 4.0 => 3 // moderate
        case _ => 4 // high-rating preference
      }
      raw_list.append(sample.user_avg_rate.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      try {
        val avg = sample.user_avg_rate
        raw_list.append(avg.toString)
        feature_list.append(1L)
        value_list.append(avg)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0F
      }
      this
    }
  }

  private class UserAvgRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val avg = try {
        sample.user_avg_rate_7day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0
      }
      // low (<3), medium (3-4), high (>=4)
      val buck = avg match {
        case x if x == 0.0 => 1
        case x if x < 3.0 => 2 // low-rating user
        case x if x < 4.0 => 3 // moderate
        case _ => 4 // high-rating preference
      }
      raw_list.append(sample.user_avg_rate_7day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserAvgRate7DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      try {
        val avg = sample.user_avg_rate_7day
        raw_list.append(avg.toString)
        feature_list.append(1L)
        value_list.append(avg)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0F
      }
      this
    }
  }

  private class UserAvgRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val avg = try {
        sample.user_avg_rate_15day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0
      }
      // low (<3), medium (3-4), high (>=4)
      val buck = avg match {
        case x if x == 0.0 => 1
        case x if x < 3.0 => 2 // low-rating user
        case x if x < 4.0 => 3 // moderate
        case _ => 4 // high-rating preference
      }
      raw_list.append(sample.user_avg_rate_15day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserAvgRate15DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      try {
        val avg = sample.user_avg_rate_15day
        raw_list.append(avg.toString)
        feature_list.append(1L)
        value_list.append(avg)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0F
      }
      this
    }
  }

  private class UserAvgRate30Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val avg = try {
        sample.user_avg_rate_30day
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0
      }
      val buck = avg match {
        case x if x == 0.0 => 1
        case x if x < 3.0 => 2
        case x if x < 4.0 => 3
        case _ => 4
      }
      raw_list.append(sample.user_avg_rate_30day.toString)
      feature_list.append(buck)
      value_list.append(1.0F)
      this
    }
  }

  private class UserAvgRate30DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      try {
        val avg = sample.user_avg_rate_30day
        raw_list.append(avg.toString)
        feature_list.append(1L)
        value_list.append(avg)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0F
      }
      this
    }
  }

  /** User's top 3 favorite genres as multi-hot feature. */
  private class UserTop3Genres(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      try {
        val topGenres = sample.user_top3_genres
        for ((g, cnt) <- topGenres) {
          if (g != null && g.nonEmpty) {
            val p = new MurmurHash3.LongPair()
            MurmurHash3.murmurhash3_x64_128(g.getBytes, 0, g.length, SEED, p)
            raw_list.append(g)
            feature_list.append(p.val1)
            value_list.append(cnt.toFloat)
          }
        }
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          }
      this
    }
  }

  /** Whether user ever watched the current movie's genres (long-term interest signal). */
  private class UserWatchSameGenre(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val hit = try {
        val genres = sample.movie_genres.toSet
        val user_genres_rate = sample.user_genres_rates.map(_._1).toSet
        val hasOverlap = if (genres.isEmpty || user_genres_rate.isEmpty) {
          1
        } else if (genres.intersect(user_genres_rate).nonEmpty) {
          2
        } else {
          1
        }
        hasOverlap
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          1
      }
      raw_list.append(hit.toString)
      feature_list.append(hit)
      value_list.append(1.0F)
      this
    }
  }

  /** Whether user watched same genre in last 1 day (short-term interest). */
  private class UserWatchSameGenre1Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_1days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 2 else 1
        raw_list.append(flag.toString)
        feature_list.append(flag)
        value_list.append(1.0F)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      this
    }
  }

  /** Whether user watched same genre in last 3 days (short-term interest). */
  private class UserWatchSameGenre3Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_3days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 2 else 1
        raw_list.append(flag.toString)
        feature_list.append(flag)
        value_list.append(1.0F)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      this
    }
  }

  /** Whether user watched same genre in last 7 days (short-term interest). */
  private class UserWatchSameGenre7Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_7days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 2 else 1
        raw_list.append(flag.toString)
        feature_list.append(flag)
        value_list.append(1.0F)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }

      this
    }
  }

  /** Whether user watched same genre in last 15 days (short-term interest). */
  private class UserWatchSameGenre15Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_15days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 2 else 1
        raw_list.append(flag.toString)
        feature_list.append(flag)
        value_list.append(1.0F)
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          0
      }
      this
    }
  }

  /** User's historical average rating for current movie's genres, bucketized. */
  private class UserSameGenreAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      var finalRate = 3.0
      val buk = try {
        val user_genre_avg_rate: Map[String, Float] = sample.user_genres_rates.toMap
        val genres: Seq[String] = sample.movie_genres
        val rates: Seq[Float] = genres.flatMap { g =>
          user_genre_avg_rate.get(g)
        }
        finalRate = if (rates.isEmpty) {
            3.0 // no history, default neutral score
        } else {
           rates.sum / rates.size // average across genres
        }
        finalRate match {
          case x if x <= 1.0 => 1
          case x if x <= 2.0 => 2
          case x if x <= 3.0 => 3
          case x if x <= 4.0 => 4
          case _ => 5
        }
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3
      }
      raw_list.append(finalRate.toString)
      feature_list.append(buk)
      value_list.append(1.0F)
      this
    }
  }

  private class UserSameGenreAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val finalRate = try {
        val userGenreAvgRate: Map[String, Float] = sample.user_genres_rates.toMap
        val rates = sample.movie_genres.flatMap(g => userGenreAvgRate.get(g))
        if (rates.isEmpty) {
          3.0F
        } else {
          rates.sum / rates.size
        }
      } catch {
        case e: Exception =>
          green_println(s"FeatureEncoder4ML1M parse error: ${e.getMessage}")
          3.0F
      }
      raw_list.append(finalRate.toString)
      feature_list.append(1L)
      value_list.append(finalRate)
      this
    }
  }

  // ============================== context ==============================

  /** Hour of day as categorical feature. */
  private class ContextTimeHour(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      raw_list.append(sample.time_hour.toString)
      feature_list.append(sample.time_hour + 1)
      value_list.append(1.0F)
      this
    }
  }

  /** Time-of-day area as categorical feature. */
  private class ContextTimeArea(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      raw_list.append(sample.time_area.toString)
      feature_list.append(sample.time_area + 1)
      value_list.append(1.0F)
      this
    }
  }

  /** Day of week as categorical feature. */
  private class ContextTimeWeek(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      raw_list.append(sample.week_day.toString)
      feature_list.append(sample.week_day)
      value_list.append(1.0F)
      this
    }
  }

  /** Weekend flag: 2 if sat/sun, 1 otherwise. */
  private class IsWeekend(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      val w = sample.week_day
      val flag = if (w == 6 || w == 7) 2 else 1
      raw_list.append(flag.toString)
      feature_list.append(flag)
      value_list.append(1.0F)
      this
    }
  }

  // ============================== user behavior ==============================

  /** User's full movie rating history sequence. */
  private class UserMovieRate(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_movie_rates.size)) {
        raw_list.append(sample.user_movie_rates(i)._1.toString)
        feature_list.append(sample.user_movie_rates(i)._1)
        value_list.append(sample.user_movie_rates(i)._2.toFloat)
      }
      this
    }
  }

  private class UserMovieRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_1days.size)) {
        raw_list.append(sample.user_movie_rate_1days(i)._1.toString)
        feature_list.append(sample.user_movie_rate_1days(i)._1)
        value_list.append(sample.user_movie_rate_1days(i)._2.toFloat)
      }
      this
    }
  }

  private class UserMovieRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_3days.size)) {
        raw_list.append(sample.user_movie_rate_3days(i)._1.toString)
        feature_list.append(sample.user_movie_rate_3days(i)._1)
        value_list.append(sample.user_movie_rate_3days(i)._2.toFloat)
      }
      this
    }
  }

  private class UserMovieRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_7days.size)) {
        raw_list.append(sample.user_movie_rate_7days(i)._1.toString)
        feature_list.append(sample.user_movie_rate_7days(i)._1)
        value_list.append(sample.user_movie_rate_7days(i)._2.toFloat)
      }
      this
    }
  }

  private class UserMovieRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_15days.size)) {
        raw_list.append(sample.user_movie_rate_15days(i)._1.toString)
        feature_list.append(sample.user_movie_rate_15days(i)._1)
        value_list.append(sample.user_movie_rate_15days(i)._2.toFloat)
      }
      this
    }
  }

  /** User's genre rating history (full). */
  private class UserGenresRate(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rates.size)) {
        val gen = sample.user_genres_rates(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(sample.user_genres_rates(i)._2.toFloat)
      }
      this
    }
  }

  private class UserGenresRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_1days.size)) {
        val gen = sample.user_genres_rate_1days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(sample.user_genres_rate_1days(i)._2.toFloat)
      }
      this
    }
  }

  private class UserGenresRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_3days.size)) {
        val gen = sample.user_genres_rate_3days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(sample.user_genres_rate_3days(i)._2.toFloat)
      }
      this
    }
  }

  private class UserGenresRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_7days.size)) {
        val gen = sample.user_genres_rate_7days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(sample.user_genres_rate_7days(i)._2.toFloat)
      }
      this
    }
  }

  private class UserGenresRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_15days.size)) {
        val gen = sample.user_genres_rate_15days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(sample.user_genres_rate_15days(i)._2.toFloat)
      }
      this
    }
  }

  /** User's genre rating count sequence (full history). */
  private class UserGenresRateCnts(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnts.size)) {
        val gen = sample.user_genres_rate_cnts(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnts(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(total_cnt.toFloat)
      }
      this
    }
  }

  private class UserGenresRateCnt1Days(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_1days.size)) {
        val gen = sample.user_genres_rate_cnt_1days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_1days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(total_cnt.toFloat)
      }
      this
    }
  }

  private class UserGenresRateCnt3Days(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_3days.size)) {
        val gen = sample.user_genres_rate_cnt_3days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_3days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(total_cnt.toFloat)
      }
      this
    }
  }

  private class UserGenresRateCnt7Days(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_7days.size)) {
        val gen = sample.user_genres_rate_cnt_7days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_7days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(total_cnt.toFloat)
      }
      this
    }
  }

  private class UserGenresRateCnt15Days(f_i: Int, f_n: String) extends CategoricalFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_15days.size)) {
        val gen = sample.user_genres_rate_cnt_15days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_15days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        raw_list.append(gen.toString)
        feature_list.append(p.val1)
        value_list.append(total_cnt.toFloat)
      }
      this
    }
  }

  override def setup(): FeatureEncoder[T] = {
    raw_cate_features.clear()
    raw_conti_features.clear()
    cross_features.clear()

    // ============================== target ==============================
    target = new Target()

    // ============================== user ==============================
    // val user_id = new UserID(1, "user_id")
    val user_age = new UserAge(2, "user_age")
    val user_gender = new UserGender(3, "user_gender")
    val user_occupation = new UserOccupation(4, "user_occupation")
    //    val user_zip_code = new UserZipCode(5, "user_zip_code")
    // raw_cate_features.append(user_id)
    raw_cate_features.append(user_age)
    raw_cate_features.append(user_gender)
    raw_cate_features.append(user_occupation)
    //    raw_cate_features.append(user_zip_code)

    val user_rate_std = new UserRateStd(6, "user_rate_std")
    val user_rate_std_7day = new UserRateStd7Day(7, "user_rate_std_7day")
    val user_rate_std_15day = new UserRateStd15Day(8, "user_rate_std_15day")
    val user_rate_std_30day = new UserRateStd30Day(9, "user_rate_std_30day")
    raw_cate_features.append(user_rate_std)
    raw_cate_features.append(user_rate_std_7day)
    raw_cate_features.append(user_rate_std_15day)
    raw_cate_features.append(user_rate_std_30day)

    val user_rate_std_continue = new UserRateStdContinue(18, "user_rate_std_continue")
    val user_rate_std_7day_continue = new UserRateStd7DayContinue(19, "user_rate_std_7day_continue")
    val user_rate_std_15day_continue = new UserRateStd15DayContinue(21, "user_rate_std_15day_continue")
    val user_rate_std_30day_continue = new UserRateStd30DayContinue(22, "user_rate_std_30day_continue")
    raw_conti_features.append(user_rate_std_continue)
    raw_conti_features.append(user_rate_std_7day_continue)
    raw_conti_features.append(user_rate_std_15day_continue)
    raw_conti_features.append(user_rate_std_30day_continue)

    val user_movie_rate_cnt = new UserMovieRateCnt(10, "user_movie_rate_cnt")
    val user_movie_rate_cnt_7day = new UserMovieRateCnt7Day(11, "user_movie_rate_cnt_7day")
    val user_movie_rate_cnt_15day = new UserMovieRateCnt15Day(12, "user_movie_rate_cnt_15day")
    val user_movie_rate_cnt_30day = new UserMovieRateCnt30Day(13, "user_movie_rate_cnt_30day")
    raw_cate_features.append(user_movie_rate_cnt)
    raw_cate_features.append(user_movie_rate_cnt_7day)
    raw_cate_features.append(user_movie_rate_cnt_15day)
    raw_cate_features.append(user_movie_rate_cnt_30day)

    val user_avg_rate = new UserAvgRate(14, "user_avg_rate")
    val user_avg_rate_7day = new UserAvgRate7Day(15, "user_avg_rate_7day")
    val user_avg_rate_15day = new UserAvgRate15Day(16, "user_avg_rate_15day")
    val user_avg_rate_30day = new UserAvgRate30Day(17, "user_avg_rate_30day")
    raw_cate_features.append(user_avg_rate)
    raw_cate_features.append(user_avg_rate_7day)
    raw_cate_features.append(user_avg_rate_15day)
    raw_cate_features.append(user_avg_rate_30day)

    val user_avg_rate_continue = new UserAvgRateContinue(23, "user_avg_rate_continue")
    val user_avg_rate_7day_continue = new UserAvgRate7DayContinue(24, "user_avg_rate_7day_continue")
    val user_avg_rate_15day_continue = new UserAvgRate15DayContinue(25, "user_avg_rate_15day_continue")
    val user_avg_rate_30day_continue = new UserAvgRate30DayContinue(26, "user_avg_rate_30day_continue")
    raw_conti_features.append(user_avg_rate_continue)
    raw_conti_features.append(user_avg_rate_7day_continue)
    raw_conti_features.append(user_avg_rate_15day_continue)
    raw_conti_features.append(user_avg_rate_30day_continue)

    // ============================== item ==============================
    // val movie_id = new MovieID(101, "movie_id")
    val movie_title = new MovieTitle(102, "movie_title")
    val movie_genres = new MovieGenres(103, "movie_genres")
    // raw_cate_features.append(movie_id)
    raw_cate_features.append(movie_title)
    raw_cate_features.append(movie_genres)

    val movie_rate_count = new MovieRateCount(104, "movie_rate_count")
    val movie_avg_rate = new MovieAvgRate(105, "movie_avg_rate")
    val movie_genre_cnt = new MovieGenreCnt(106, "movie_genre_cnt")
    val movie_hot_rank = new MovieHotRank(107, "item_hot_rank")
    val movie_publish_year = new MoviePublishYear(108, "movie_publish_year")
    val movie_avg_rate_continue = new MovieAvgRateContinue(109, "movie_avg_rate_continue")
    raw_cate_features.append(movie_rate_count)
    raw_cate_features.append(movie_avg_rate)
    raw_cate_features.append(movie_genre_cnt)
    raw_cate_features.append(movie_hot_rank)
    raw_cate_features.append(movie_publish_year)
    raw_conti_features.append(movie_avg_rate_continue)


    // ============================== context ==============================
    val context_time_hour = new ContextTimeHour(201, "context_time_hour")
    val context_time_area = new ContextTimeArea(202, "context_time_area")
    val context_time_week = new ContextTimeWeek(203, "context_time_week")
    val context_is_weekend = new IsWeekend(204, "context_is_weekend")
    raw_cate_features.append(context_time_hour)
    raw_cate_features.append(context_time_area)
    raw_cate_features.append(context_time_week)
    raw_cate_features.append(context_is_weekend)


    // ============================== user behavior ==============================
    val user_movie_rate = new UserMovieRate(301, "user_movie_rate")
    val user_movie_rate_1day = new UserMovieRate1Day(302, "user_movie_rate_1day")
    val user_movie_rate_3day = new UserMovieRate3Day(303, "user_movie_rate_3day")
    val user_movie_rate_7day = new UserMovieRate7Day(304, "user_movie_rate_7day")
    val user_movie_rate_15day = new UserMovieRate15Day(305, "user_movie_rate_15day")
    raw_cate_features.append(user_movie_rate)
    raw_cate_features.append(user_movie_rate_1day)
    raw_cate_features.append(user_movie_rate_3day)
    raw_cate_features.append(user_movie_rate_7day)
    raw_cate_features.append(user_movie_rate_15day)

    val user_genres_rate = new UserGenresRate(306, "user_genres_rate")
    val user_genres_rate_1day = new UserGenresRate1Day(307, "user_genres_rate_1day")
    val user_genres_rate_3day = new UserGenresRate3Day(308, "user_genres_rate_3day")
    val user_genres_rate_7day = new UserGenresRate7Day(309, "user_genres_rate_7day")
    val user_genres_rate_15day = new UserGenresRate15Day(310, "user_genres_rate_15day")
    raw_cate_features.append(user_genres_rate)
    raw_cate_features.append(user_genres_rate_1day)
    raw_cate_features.append(user_genres_rate_3day)
    raw_cate_features.append(user_genres_rate_7day)
    raw_cate_features.append(user_genres_rate_15day)

    val user_genres_rate_cnts = new UserGenresRateCnts(312, "user_genres_rate_cnts")
    val user_genres_rate_cnt_1days = new UserGenresRateCnt1Days(313, "user_genres_rate_cnt_1days")
    val user_genres_rate_cnt_3days = new UserGenresRateCnt3Days(314, "user_genres_rate_cnt_3days")
    val user_genres_rate_cnt_7days = new UserGenresRateCnt7Days(315, "user_genres_rate_cnt_7days")
    val user_genres_rate_cnt_15days = new UserGenresRateCnt15Days(316, "user_genres_rate_cnt_15days")
    raw_cate_features.append(user_genres_rate_cnts)
    raw_cate_features.append(user_genres_rate_cnt_1days)
    raw_cate_features.append(user_genres_rate_cnt_3days)
    raw_cate_features.append(user_genres_rate_cnt_7days)
    raw_cate_features.append(user_genres_rate_cnt_15days)

    val user_top3_genres = new UserTop3Genres(317, "user_top3_genres")
    raw_cate_features.append(user_top3_genres)

    val user_watch_same_genre = new UserWatchSameGenre(351, "user_watch_same_genre")
    val user_watch_same_genre_1day = new UserWatchSameGenre1Day(352, "user_watch_same_genre_1day")
    val user_watch_same_genre_3day = new UserWatchSameGenre3Day(353, "user_watch_same_genre_3day")
    val user_watch_same_genre_7day = new UserWatchSameGenre7Day(354, "user_watch_same_genre_7day")
    val user_watch_same_genre_15day = new UserWatchSameGenre15Day(355, "user_watch_same_genre_15day")
    val user_same_genre_avg_rate = new UserSameGenreAvgRate(356, "user_same_genre_avg_rate")
    val user_same_genre_avg_rate_continue = new UserSameGenreAvgRateContinue(357, "user_same_genre_avg_rate_continue")
    raw_cate_features.append(user_watch_same_genre)
    raw_cate_features.append(user_watch_same_genre_1day)
    raw_cate_features.append(user_watch_same_genre_3day)
    raw_cate_features.append(user_watch_same_genre_7day)
    raw_cate_features.append(user_watch_same_genre_15day)
    raw_cate_features.append(user_same_genre_avg_rate)
    raw_conti_features.append(user_same_genre_avg_rate_continue)

    // Cross features disabled for local end-to-end testing; enable for production.
    val enableCrossFeatures = false
    if (enableCrossFeatures) {
      // ============================== second-order cross features ==============================
      // Genre x user full-history genre preference (long-term interest matching)
      val movie_genres_xx_user_genres_rate = new CrossFeature(401, "movie_genres_xx_user_genres_rate", movie_genres, user_genres_rate)
      // Genre x user 1-day genre preference (ultra-short-term interest)
      val movie_genres_xx_user_genres_rate_1day = new CrossFeature(402, "movie_genres_xx_user_genres_rate_1day", movie_genres, user_genres_rate_1day)
      // Genre x user 3-day genre preference (short-term interest)
      val movie_genres_xx_user_genres_rate_3day = new CrossFeature(403, "movie_genres_xx_user_genres_rate_3day", movie_genres, user_genres_rate_3day)
      // Genre x user 7-day genre preference (mid-term interest)
      val movie_genres_xx_user_genres_rate_7day = new CrossFeature(404, "movie_genres_xx_user_genres_rate_7day", movie_genres, user_genres_rate_7day)
      // Genre x user 15-day genre preference (mid-to-long-term interest)
      val movie_genres_xx_user_genres_rate_15day = new CrossFeature(405, "movie_genres_xx_user_genres_rate_15day", movie_genres, user_genres_rate_15day)
      cross_features.append(movie_genres_xx_user_genres_rate)
      cross_features.append(movie_genres_xx_user_genres_rate_1day)
      cross_features.append(movie_genres_xx_user_genres_rate_3day)
      cross_features.append(movie_genres_xx_user_genres_rate_7day)
      cross_features.append(movie_genres_xx_user_genres_rate_15day)

      // Item base x user base: publish year x age (era + demographic match)
      val movie_publish_year_xx_user_age = new CrossFeature(406, "movie_publish_year_xx_user_age", movie_publish_year, user_age)
      cross_features.append(movie_publish_year_xx_user_age)

      // Item stats x user preference: rate count x rating std (popularity x pickiness)
      val movie_rate_count_xx_user_rate_std = new CrossFeature(410, "movie_rate_count_xx_user_rate_std", movie_rate_count, user_rate_std)
      // Hot rank x user avg rate (popularity x rating taste)
      val movie_hot_rank_xx_user_avg_rate = new CrossFeature(411, "movie_hot_rank_xx_user_avg_rate", movie_hot_rank, user_avg_rate)
      // Publish year x user avg rate (era preference x rating style)
      val movie_publish_year_xx_user_avg_rate = new CrossFeature(412, "movie_publish_year_xx_user_avg_rate", movie_publish_year, user_avg_rate)
      // Genre count x user avg rate (complexity x rating preference)
      val movie_genre_cnt_xx_user_avg_rate = new CrossFeature(413, "movie_genre_cnt_xx_user_avg_rate", movie_genre_cnt, user_avg_rate)
      // Hot rank x user genre avg rate (popularity x niche taste)
      val movie_hot_rank_xx_user_genre_avg_rate = new CrossFeature(414, "movie_hot_rank_xx_user_genre_avg_rate", movie_hot_rank, user_same_genre_avg_rate)
      cross_features.append(movie_rate_count_xx_user_rate_std)
      cross_features.append(movie_hot_rank_xx_user_avg_rate)
      cross_features.append(movie_publish_year_xx_user_avg_rate)
      cross_features.append(movie_genre_cnt_xx_user_avg_rate)
      cross_features.append(movie_hot_rank_xx_user_genre_avg_rate)

      // Genre x user demographics: gender x genre (strong gender preference)
      val movie_genres_xx_user_gender = new CrossFeature(417, "movie_genres_xx_user_gender", movie_genres, user_gender)
      // Occupation x genre (professional circle preference)
      val movie_genres_xx_user_occupation = new CrossFeature(418, "movie_genres_xx_user_occupation", movie_genres, user_occupation)
      // Genre x age (age-based genre preference varies significantly)
      val movie_genres_xx_user_age = new CrossFeature(419, "movie_genres_xx_user_age", movie_genres, user_age)
      cross_features.append(movie_genres_xx_user_gender)
      cross_features.append(movie_genres_xx_user_occupation)
      cross_features.append(movie_genres_xx_user_age)

      // Holiday-related: genre x weekend (weekend/weekday genre divergence)
      val movie_genres_xx_is_weekend = new CrossFeature(450, "movie_genres_xx_is_weekend", movie_genres, context_is_weekend)
      // Hot rank x weekend (blockbusters on weekends, niche on weekdays)
      val movie_hot_rank_xx_is_weekend = new CrossFeature(451, "movie_hot_rank_xx_is_weekend", movie_hot_rank, context_is_weekend)
      // Age x weekend (age-based weekend viewing patterns)
      val user_age_xx_is_weekend = new CrossFeature(452, "user_age_xx_is_weekend", user_age, context_is_weekend)
      // Gender x time hour (male/female viewing time preference)
      val user_gender_xx_context_time_hour = new CrossFeature(453, "user_gender_xx_context_time_hour", user_gender, context_time_hour)
      cross_features.append(movie_genres_xx_is_weekend)
      cross_features.append(movie_hot_rank_xx_is_weekend)
      cross_features.append(user_age_xx_is_weekend)
      cross_features.append(user_gender_xx_context_time_hour)

      // ============================== third-order cross features ==============================
      // Age x gender x genre (core demographic segmentation)
      val movie_genres_xx_user_age_xx_user_gender = new CrossFeature(460, "movie_genres_xx_user_age_xx_user_gender", movie_genres, user_age, user_gender)
      // Age x occupation x publish year (era preference stratification)
      val movie_publish_year_xx_user_age_xx_user_occupation = new CrossFeature(462, "movie_publish_year_xx_user_age_xx_user_occupation", movie_publish_year, user_age, user_occupation)
      // Gender x occupation x genre (fine-grained demographic feature)
      val movie_genres_xx_user_gender_xx_user_occupation = new CrossFeature(461, "movie_genres_xx_user_gender_xx_user_occupation", movie_genres, user_gender, user_occupation)
      // Movie avg rate x hot rank x user avg rate (taste matching)
      val movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate = new CrossFeature(463, "movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate", movie_avg_rate, movie_hot_rank, user_avg_rate)
      // Genre count x total rate count x user avg rate (depth + taste)
      val movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate = new CrossFeature(465, "movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate", movie_genre_cnt, user_movie_rate_cnt, user_avg_rate)
      // Genre count x hot rank x user genre avg rate (genre preference + popularity)
      val movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate = new CrossFeature(466, "movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate", movie_genre_cnt, movie_hot_rank, user_same_genre_avg_rate)
      // Publish year x avg rate x user avg rate (era + quality + taste)
      val movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate = new CrossFeature(467, "movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate", movie_publish_year, movie_avg_rate, user_avg_rate)

      cross_features.append(movie_genres_xx_user_age_xx_user_gender)
      cross_features.append(movie_publish_year_xx_user_age_xx_user_occupation)
      cross_features.append(movie_genres_xx_user_gender_xx_user_occupation)
      cross_features.append(movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate)
      cross_features.append(movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate)
      cross_features.append(movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate)
      cross_features.append(movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate)
    }

    return this
  }
}
