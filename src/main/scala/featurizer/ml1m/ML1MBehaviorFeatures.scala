package featurizer.ml1m

import featurizer.{CategoricalFeature, RawFeature}
import utils.MurmurHash3

import scala.collection.mutable.ArrayBuffer

class UserMovieRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rates
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_1days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_3days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_7days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_15days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

class UserGenresRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rates
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

class UserGenresRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_1days
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

class UserGenresRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_3days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(seq(i)._2)
    }
    this
  }
}

class UserGenresRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_7days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(seq(i)._2)
    }
    this
  }
}

class UserGenresRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_15days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(seq(i)._2)
    }
    this
  }
}

class UserGenresRateCnts(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnts
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val total_cnt = seq(i)._2
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt)
    }
    this
  }
}

class UserGenresRateCnt1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnt_1days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
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

class UserGenresRateCnt7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnt_7days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
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
