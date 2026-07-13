package featurizer.ml1m

import scala.collection.mutable.ArrayBuffer
import featurizer.{CategoricalFeature, ContinuousFeature, RawFeature}
import utils.MurmurHash3
import utils.LogUtils.green_println


trait BucketedStdLike {
  this: CategoricalFeature[ML1MSample] =>
  def getValue(sample: ML1MSample): Float

  override def parse(sample: ML1MSample): RawFeature = {
    val v = try {
      getValue(sample)
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0.0
    }
    val buck = v match {
      case x if x <= 0.0 => 1
      case x if x <= 1.0 => 2
      case x if x <= 2.0 => 3
      case _ => 4
    }
    raw_list.append(v.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

trait ContinuousStdLike {
  this: ContinuousFeature[ML1MSample] =>
  def getValue(sample: ML1MSample): Float

  override def parse(sample: ML1MSample): RawFeature = {
    val v = try {
      getValue(sample)
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0.0F
    }
    raw_list.append(v.toString)
    feature_list.append(1L)
    value_list.append(v)
    this
  }
}

trait BucketedAvgRateLike {
  this: CategoricalFeature[ML1MSample] =>
  def getValue(sample: ML1MSample): Float

  override def parse(sample: ML1MSample): RawFeature = {
    val v = try {
      getValue(sample)
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 3.0
    }
    val buck = v match {
      case x if x == 0.0 => 1
      case x if x < 3.0 => 2
      case x if x < 4.0 => 3
      case _ => 4
    }
    raw_list.append(v.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

trait ContinuousAvgRateLike {
  this: ContinuousFeature[ML1MSample] =>
  def getValue(sample: ML1MSample): Float

  override def parse(sample: ML1MSample): RawFeature = {
    val v = try {
      getValue(sample)
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 3.0F
    }
    raw_list.append(v.toString)
    feature_list.append(1L)
    value_list.append(v)
    this
  }
}

trait BucketedCountLike {
  this: CategoricalFeature[ML1MSample] =>
  def getValue(sample: ML1MSample): Int

  override def parse(sample: ML1MSample): RawFeature = {
    val v = try {
      getValue(sample)
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0
    }
    val buck = v match {
      case x if x == 0 => 1
      case x if x <= 10 => 2
      case x if x <= 30 => 3
      case x if x <= 50 => 4
      case x if x <= 100 => 5
      case _ => 6
    }
    raw_list.append(v.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

trait WatchSameGenreLike {
  this: CategoricalFeature[ML1MSample] =>
  def getGenres(sample: ML1MSample): ArrayBuffer[(String, Float)]

  override def parse(sample: ML1MSample): RawFeature = {
    val hit = try {
      val currentGenres = sample.movie_genres.toSet
      val recentGenres = getGenres(sample).map(_._1).toSet
      if (currentGenres.intersect(recentGenres).nonEmpty) 2 else 1
    } catch {
      case e: Exception =>
        green_println(s"Featurizer4ML1M parse error: ${e.getMessage}")
        1
    }
    raw_list.append(hit.toString)
    feature_list.append(hit)
    value_list.append(1.0F)
    this
  }
}

class UserID(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val user_id = try {
      sample.user_id.toInt
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}");
        0
    }
    raw_list.append(sample.user_id)
    feature_list.append(user_id)
    value_list.append(1.0F)
    this
  }
}

class UserAge(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val age = try {
      sample.age.toInt
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0
    }
    raw_list.append(sample.age)
    feature_list.append(age)
    value_list.append(1.0F)
    this
  }
}

class UserGender(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val buck = sample.gender match {
      case "M" => 1;
      case "F" => 2;
      case _ => 0
    }
    raw_list.append(sample.gender)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class UserOccupation(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val occupation = try {
      sample.occupation.toInt
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0
    }
    raw_list.append(sample.occupation)
    feature_list.append(occupation)
    value_list.append(1.0F)
    this
  }
}

class UserZipCode(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val hash = try {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(sample.zip_code.getBytes(), 0, sample.zip_code.length, SEED, p)
      p.val1
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0L
    }
    raw_list.append(sample.zip_code)
    feature_list.append(hash)
    value_list.append(1.0F)
    this
  }
}

class UserRateStd(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std
}

class UserRateStdContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std
}

class UserRateStd7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std_7day
}

class UserRateStd7DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std_7day
}

class UserRateStd15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std_15day
}

class UserRateStd15DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std_15day
}

class UserRateStd30Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std_30day
}

class UserRateStd30DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: ML1MSample): Float = sample.user_rate_std_30day
}

class UserActiveDay(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val days = try {
      sample.user_active_day
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 0
    }
    val buck = days match {
      case 0 => 1;
      case x if x <= 7 => 2;
      case x if x <= 30 => 3;
      case _ => 4
    }
    raw_list.append(days.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class UserMovieRateCnt(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: ML1MSample): Int = sample.user_rate_cnt
}

class UserMovieRateCnt7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: ML1MSample): Int = sample.user_rate_7day_cnt
}

class UserMovieRateCnt15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: ML1MSample): Int = sample.user_rate_15day_cnt
}

class UserMovieRateCnt30Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: ML1MSample): Int = sample.user_rate_30day_cnt
}

class UserAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate
}

class UserAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate
}

class UserAvgRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate_7day
}

class UserAvgRate7DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate_7day
}

class UserAvgRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate_15day
}

class UserAvgRate15DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate_15day
}

class UserAvgRate30Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate_30day
}

class UserAvgRate30DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: ML1MSample): Float = sample.user_avg_rate_30day
}

class UserTop3Genres(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}")
    }
    this
  }
}

class UserWatchSameGenre(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with WatchSameGenreLike {
  def getGenres(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rates
}

class UserWatchSameGenre1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with WatchSameGenreLike {
  def getGenres(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_1days
}

class UserWatchSameGenre3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with WatchSameGenreLike {
  def getGenres(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_3days
}

class UserWatchSameGenre7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with WatchSameGenreLike {
  def getGenres(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_7days
}

class UserWatchSameGenre15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with WatchSameGenreLike {
  def getGenres(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_15days
}

class UserSameGenreAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    var finalRate = 3.0
    val buk = try {
      val userGenreAvgRate: Map[String, Float] = sample.user_genres_rates.toMap
      val rates = sample.movie_genres.flatMap(g => userGenreAvgRate.get(g))
      finalRate = if (rates.isEmpty) 3.0 else rates.sum / rates.size
      finalRate match {
        case x if x <= 1.0 => 1;
        case x if x <= 2.0 => 2
        case x if x <= 3.0 => 3;
        case x if x <= 4.0 => 4;
        case _ => 5
      }
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 3
    }
    raw_list.append(finalRate.toString)
    feature_list.append(buk)
    value_list.append(1.0F)
    this
  }
}

class UserSameGenreAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val finalRate = try {
      val userGenreAvgRate: Map[String, Float] = sample.user_genres_rates.toMap
      val rates = sample.movie_genres.flatMap(g => userGenreAvgRate.get(g))
      if (rates.isEmpty) 3.0F else rates.sum / rates.size
    } catch {
      case e: Exception => green_println(s"Featurizer4ML1M parse error: ${e.getMessage}"); 3.0F
    }
    raw_list.append(finalRate.toString)
    feature_list.append(1L)
    value_list.append(finalRate)
    this
  }
}
