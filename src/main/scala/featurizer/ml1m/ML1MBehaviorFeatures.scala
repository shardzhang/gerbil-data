package featurizer.ml1m

import utils.MurmurHash3
import featurizer.ml1m.ML1MSample
import featurizer.core.{CategoricalFeature, RawFeature}

class UserMovieRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    for (i <- 0 until Math.min(200, sample.user_movie_rates.size)) {
      raw_list.append(sample.user_movie_rates(i)._1.toString)
      feature_list.append(sample.user_movie_rates(i)._1)
      value_list.append(sample.user_movie_rates(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    for (i <- 0 until Math.min(200, sample.user_movie_rate_1days.size)) {
      raw_list.append(sample.user_movie_rate_1days(i)._1.toString)
      feature_list.append(sample.user_movie_rate_1days(i)._1)
      value_list.append(sample.user_movie_rate_1days(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    for (i <- 0 until Math.min(200, sample.user_movie_rate_3days.size)) {
      raw_list.append(sample.user_movie_rate_3days(i)._1.toString)
      feature_list.append(sample.user_movie_rate_3days(i)._1)
      value_list.append(sample.user_movie_rate_3days(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    for (i <- 0 until Math.min(200, sample.user_movie_rate_7days.size)) {
      raw_list.append(sample.user_movie_rate_7days(i)._1.toString)
      feature_list.append(sample.user_movie_rate_7days(i)._1)
      value_list.append(sample.user_movie_rate_7days(i)._2.toFloat)
    }
    this
  }
}

class UserMovieRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    for (i <- 0 until Math.min(200, sample.user_movie_rate_15days.size)) {
      raw_list.append(sample.user_movie_rate_15days(i)._1.toString)
      feature_list.append(sample.user_movie_rate_15days(i)._1)
      value_list.append(sample.user_movie_rate_15days(i)._2.toFloat)
    }
    this
  }
}

class UserGenresRate(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRateCnts(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRateCnt1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRateCnt3Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRateCnt7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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

class UserGenresRateCnt15Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
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
