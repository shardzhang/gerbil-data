package featurizer.mobilerec

import featurizer.{CategoricalFeature, ContinuousFeature, RawFeature}

import scala.collection.mutable.ArrayBuffer

trait BucketedStdLike {
  this: CategoricalFeature[MobileRecSample] =>
  def getValue(sample: MobileRecSample): Float

  override def parse(sample: MobileRecSample): RawFeature = {
    val v = getValue(sample)
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
  this: ContinuousFeature[MobileRecSample] =>
  def getValue(sample: MobileRecSample): Float

  override def parse(sample: MobileRecSample): RawFeature = {
    val v = getValue(sample)
    raw_list.append(v.toString)
    feature_list.append(1L)
    value_list.append(v)
    this
  }
}

trait BucketedAvgRateLike {
  this: CategoricalFeature[MobileRecSample] =>
  def getValue(sample: MobileRecSample): Float

  override def parse(sample: MobileRecSample): RawFeature = {
    val v = getValue(sample)
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
  this: ContinuousFeature[MobileRecSample] =>
  def getValue(sample: MobileRecSample): Float

  override def parse(sample: MobileRecSample): RawFeature = {
    val v = getValue(sample)
    raw_list.append(v.toString)
    feature_list.append(1L)
    value_list.append(v)
    this
  }
}

trait BucketedCountLike {
  this: CategoricalFeature[MobileRecSample] =>
  def getValue(sample: MobileRecSample): Int

  override def parse(sample: MobileRecSample): RawFeature = {
    val v = getValue(sample)
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

class MobileRecUserID(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val p = new utils.MurmurHash3.LongPair()
    utils.MurmurHash3.murmurhash3_x64_128(sample.uid.getBytes, 0, sample.uid.length, SEED, p)
    raw_list.append(sample.uid)
    feature_list.append(p.val1)
    value_list.append(1.0F)
    this
  }
}

class MobileRecUserRateStd(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std
}

class MobileRecUserRateStdContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std
}

class MobileRecUserRateStd7Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std_7day
}

class MobileRecUserRateStd7DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std_7day
}

class MobileRecUserRateStd15Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std_15day
}

class MobileRecUserRateStd15DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std_15day
}

class MobileRecUserRateStd30Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std_30day
}

class MobileRecUserRateStd30DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousStdLike {
  def getValue(sample: MobileRecSample): Float = sample.user_rate_std_30day
}

class MobileRecUserMovieRateCnt(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: MobileRecSample): Int = sample.user_rate_cnt
}

class MobileRecUserMovieRateCnt7Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: MobileRecSample): Int = sample.user_rate_7day_cnt
}

class MobileRecUserMovieRateCnt15Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: MobileRecSample): Int = sample.user_rate_15day_cnt
}

class MobileRecUserMovieRateCnt30Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedCountLike {
  def getValue(sample: MobileRecSample): Int = sample.user_rate_30day_cnt
}

class MobileRecUserAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate
}

class MobileRecUserAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate
}

class MobileRecUserAvgRate7Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate_7day
}

class MobileRecUserAvgRate7DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate_7day
}

class MobileRecUserAvgRate15Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate_15day
}

class MobileRecUserAvgRate15DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate_15day
}

class MobileRecUserAvgRate30Day(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) with BucketedAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate_30day
}

class MobileRecUserAvgRate30DayContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) with ContinuousAvgRateLike {
  def getValue(sample: MobileRecSample): Float = sample.user_avg_rate_30day
}

class MobileRecUserTop3Categories(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    for ((g, cnt) <- sample.user_top3_categories) {
      if (g != null && g.nonEmpty) {
        val p = new utils.MurmurHash3.LongPair()
        utils.MurmurHash3.murmurhash3_x64_128(g.getBytes, 0, g.length, SEED, p)
        raw_list.append(g)
        feature_list.append(p.val1)
        value_list.append(cnt.toFloat)
      }
    }
    this
  }
}

class MobileRecUserActiveDay(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val buck = sample.user_active_day match {
      case 0 => 1
      case x if x <= 7 => 2
      case x if x <= 30 => 3
      case _ => 4
    }
    raw_list.append(sample.user_active_day.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}
