package featurizer.ml1m

import utils.MurmurHash3
import featurizer.core.{CategoricalFeature, RawFeature}

import scala.collection.mutable.ArrayBuffer

trait UserMovieRateLike {
  this: CategoricalFeature[ML1MSample] =>
  def getSeq(sample: ML1MSample): ArrayBuffer[(Int, Int)]

  override def parse(sample: ML1MSample): RawFeature = {
    val seq = getSeq(sample)
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

trait UserGenresRateLike {
  this: CategoricalFeature[ML1MSample] =>
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Float)]

  override def parse(sample: ML1MSample): RawFeature = {
    val seq = getSeq(sample)
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

trait UserGenresRateCntsLike {
  this: CategoricalFeature[ML1MSample] =>
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Int)]

  override def parse(sample: ML1MSample): RawFeature = {
    val seq = getSeq(sample)
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase()
      val total_cnt = seq(i)._2
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt.toFloat)
    }
    this
  }
}

class UserMovieRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserMovieRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(Int, Int)] = sample.user_movie_rates
}

class UserMovieRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserMovieRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(Int, Int)] = sample.user_movie_rate_1days
}

class UserMovieRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserMovieRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(Int, Int)] = sample.user_movie_rate_3days
}

class UserMovieRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserMovieRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(Int, Int)] = sample.user_movie_rate_7days
}

class UserMovieRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserMovieRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(Int, Int)] = sample.user_movie_rate_15days
}

class UserGenresRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rates
}

class UserGenresRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_1days
}

class UserGenresRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_3days
}

class UserGenresRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_7days
}

class UserGenresRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Float)] = sample.user_genres_rate_15days
}

class UserGenresRateCnts(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateCntsLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Int)] = sample.user_genres_rate_cnts
}

class UserGenresRateCnt1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateCntsLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Int)] = sample.user_genres_rate_cnt_1days
}

class UserGenresRateCnt3Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateCntsLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Int)] = sample.user_genres_rate_cnt_3days
}

class UserGenresRateCnt7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateCntsLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Int)] = sample.user_genres_rate_cnt_7days
}

class UserGenresRateCnt15Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) with UserGenresRateCntsLike {
  def getSeq(sample: ML1MSample): ArrayBuffer[(String, Int)] = sample.user_genres_rate_cnt_15days
}
