package featurizer.mobilerec

import utils.MurmurHash3
import featurizer.core.{CategoricalFeature, RawFeature}

import scala.collection.mutable.ArrayBuffer

trait UserAppsRateLike {
  this: CategoricalFeature[MobileRecSample] =>
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)]

  override def parse(sample: MobileRecSample): RawFeature = {
    val seq = getSeq(sample)
    for (i <- 0 until Math.min(200, seq.size)) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(seq(i)._1.getBytes, 0, seq(i)._1.length, SEED, p)
      raw_list.append(seq(i)._1)
      feature_list.append(p.val1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

trait UserCategoriesRateLike {
  this: CategoricalFeature[MobileRecSample] =>
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)]

  override def parse(sample: MobileRecSample): RawFeature = {
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

trait UserCategoriesRateCntsLike {
  this: CategoricalFeature[MobileRecSample] =>
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)]

  override def parse(sample: MobileRecSample): RawFeature = {
    val seq = getSeq(sample)
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

class MobileRecUserAppsRate(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserAppsRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_apps_rates
}

class MobileRecUserAppsRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserAppsRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_apps_rate_1days
}

class MobileRecUserAppsRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserAppsRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_apps_rate_3days
}

class MobileRecUserAppsRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserAppsRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_apps_rate_7days
}

class MobileRecUserAppsRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserAppsRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_apps_rate_15days
}

class MobileRecUserAppsRate30Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserAppsRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_apps_rate_30days
}

class MobileRecUserCategoriesRate(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)] = sample.user_categories_rates
}

class MobileRecUserCategoriesRate1Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)] = sample.user_categories_rate_1days
}

class MobileRecUserCategoriesRate3Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)] = sample.user_categories_rate_3days
}

class MobileRecUserCategoriesRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)] = sample.user_categories_rate_7days
}

class MobileRecUserCategoriesRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)] = sample.user_categories_rate_15days
}

class MobileRecUserCategoriesRate30Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Float)] = sample.user_categories_rate_30days
}

class MobileRecUserCategoriesRateCnts(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateCntsLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_categories_rate_cnts
}

class MobileRecUserCategoriesRateCnt1Days(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateCntsLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_categories_rate_cnt_1days
}

class MobileRecUserCategoriesRateCnt3Days(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateCntsLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_categories_rate_cnt_3days
}

class MobileRecUserCategoriesRateCnt7Days(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateCntsLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_categories_rate_cnt_7days
}

class MobileRecUserCategoriesRateCnt15Days(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateCntsLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_categories_rate_cnt_15days
}

class MobileRecUserCategoriesRateCnt30Days(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with UserCategoriesRateCntsLike {
  def getSeq(sample: MobileRecSample): ArrayBuffer[(String, Int)] = sample.user_categories_rate_cnt_30days
}
