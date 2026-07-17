package featurizer.ml1m

import featurizer.{CategoricalFeature, RawFeature}
import utils.MurmurHash3

import scala.collection.mutable.ArrayBuffer

/** 和Item genre拼接后, 做target-attention */
class UserMovieRateIds(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_ids
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

/** 和ItemId拼接后, 做target-attention */
class UserMovieRateGenres(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_genres
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val rate = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(rate.toFloat)
    }
    this
  }
}

/** 和Item genre拼接后, 做target-attention */
class UserMovieRateId1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_id_1days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

/** 和ItemId拼接后, 做target-attention */
class UserMovieRateGenre1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_genre_1days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val rate = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(rate.toFloat)
    }
    this
  }
}

/** 和Item genre拼接后, 做target-attention */
class UserMovieRateId3Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_id_3days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

/** 和ItemId拼接后, 做target-attention */
class UserMovieRateGenre3Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_genre_3days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val rate = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(rate.toFloat)
    }
    this
  }
}

/** 和Item genre拼接后, 做target-attention */
class UserMovieRateId7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_id_7days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

/** 和ItemId拼接后, 做target-attention */
class UserMovieRateGenre7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_genre_7days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val rate = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(rate.toFloat)
    }
    this
  }
}

/** 和Item genre拼接后, 做target-attention */
class UserMovieRateId15Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_id_15days
    for (i <- 0 until Math.min(200, seq.size)) {
      raw_list.append(seq(i)._1.toString)
      feature_list.append(seq(i)._1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

/** 和ItemId拼接后, 做target-attention */
class UserMovieRateGenre15Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_movie_rate_genre_15days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val rate = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(rate.toFloat)
    }
    this
  }
}

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRates(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
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

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRate1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
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

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRate3Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
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

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRate7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
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

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRate15Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
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

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRateCnts(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnts
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val total_cnt = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt)
    }
    this
  }
}

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRateCnt1Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnt_1days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val total_cnt = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt.toFloat)
    }
    this
  }
}

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRateCnt7Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnt_7days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val total_cnt = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt.toFloat)
    }
    this
  }
}

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRateCnt15Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnt_15days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val total_cnt = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt.toFloat)
    }
    this
  }
}

/** 仅行为序列特征, 不做target attentsion */
class UserGenresRateCnt30Days(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val seq = sample.user_genres_rate_cnt_30days
    for (i <- 0 until Math.min(200, seq.size)) {
      val gen = seq(i)._1.trim.toLowerCase
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
      val total_cnt = seq(i)._2
      raw_list.append(gen)
      feature_list.append(p.val1)
      value_list.append(total_cnt.toFloat)
    }
    this
  }
}
