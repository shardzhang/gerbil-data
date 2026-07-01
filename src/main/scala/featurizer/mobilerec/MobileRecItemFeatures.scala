package featurizer.mobilerec

import featurizer.core.{CategoricalFeature, ContinuousFeature, RawFeature}
import utils.MurmurHash3

import scala.collection.mutable.ArrayBuffer

class AppPackage(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val p = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(sample.app_package.getBytes, 0, sample.app_package.length, SEED, p)
    raw_list.append(sample.app_package)
    feature_list.append(p.val1)
    value_list.append(1.0F)
    this
  }
}

class AppCategory(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    if (sample.app_category != null && sample.app_category.nonEmpty) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(sample.app_category.getBytes, 0, sample.app_category.length, SEED, p)
      raw_list.append(sample.app_category)
      feature_list.append(p.val1)
      value_list.append(1.0F)
    }
    this
  }
}

class AppPriceBucket(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val buck = sample.app_price match {
      case x if x <= 0.0 => 1
      case x if x <= 1.0 => 2
      case x if x <= 5.0 => 3
      case _ => 4
    }
    raw_list.append(sample.app_price.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class AppPrice(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    raw_list.append(sample.app_price.toString)
    feature_list.append(1L)
    value_list.append(sample.app_price.toFloat)
    this
  }
}

class AppAvgRate(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val buck = sample.app_avg_rating match {
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
    raw_list.append(sample.app_avg_rating.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class AppAvgRateContinue(f_i: Int, f_n: String) extends ContinuousFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    raw_list.append(sample.app_avg_rating.toString)
    feature_list.append(1L)
    value_list.append(sample.app_avg_rating.toFloat)
    this
  }
}

class AppReviewCnt(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val buck = sample.app_num_reviews match {
      case x if x <= 0 => 1
      case x if x <= 10 => 2
      case x if x <= 50 => 3
      case x if x <= 100 => 4
      case x if x <= 500 => 5
      case x if x <= 1000 => 6
      case x if x <= 5000 => 7
      case x if x <= 10000 => 8
      case x if x <= 50000 => 9
      case _ => 10
    }
    raw_list.append(sample.app_num_reviews.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class AppContentRating(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val buck = sample.app_content_rating match {
      case "Everyone" => 1
      case "Teen" => 2
      case "Mature 17+" => 3
      case "Adults only 18+" => 4
      case _ => 0
    }
    raw_list.append(sample.app_content_rating)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}
