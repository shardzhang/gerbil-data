package featurizer.alictr

import featurizer.core.{CategoricalFeature, ContinuousFeature, RawFeature}
import utils.MurmurHash3

class AliCtrAdgroupID(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val p = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(sample.adgroup_id.getBytes, 0, sample.adgroup_id.length, SEED, p)
    raw_list.append(sample.adgroup_id)
    feature_list.append(p.val1)
    value_list.append(1.0F)
    this
  }
}

class AliCtrCateID(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    if (sample.cate_id != null && sample.cate_id.nonEmpty) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(sample.cate_id.getBytes, 0, sample.cate_id.length, SEED, p)
      raw_list.append(sample.cate_id)
      feature_list.append(p.val1)
      value_list.append(1.0F)
    }
    this
  }
}

class AliCtrCampaignID(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    if (sample.campaign_id != null && sample.campaign_id.nonEmpty) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(sample.campaign_id.getBytes, 0, sample.campaign_id.length, SEED, p)
      raw_list.append(sample.campaign_id)
      feature_list.append(p.val1)
      value_list.append(1.0F)
    }
    this
  }
}

class AliCtrCustomer(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    if (sample.customer != null && sample.customer.nonEmpty) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(sample.customer.getBytes, 0, sample.customer.length, SEED, p)
      raw_list.append(sample.customer)
      feature_list.append(p.val1)
      value_list.append(1.0F)
    }
    this
  }
}

class AliCtrBrand(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    if (sample.brand != null && sample.brand.nonEmpty) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(sample.brand.getBytes, 0, sample.brand.length, SEED, p)
      raw_list.append(sample.brand)
      feature_list.append(p.val1)
      value_list.append(1.0F)
    }
    this
  }
}

class AliCtrPrice(f_i: Int, f_n: String) extends ContinuousFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    raw_list.append(sample.price.toString)
    feature_list.append(1L)
    value_list.append(sample.price.toFloat)
    this
  }
}

class AliCtrPriceBucket(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val buck = sample.price match {
      case x if x <= 0.0 => 1
      case x if x <= 10.0 => 2
      case x if x <= 50.0 => 3
      case x if x <= 100.0 => 4
      case x if x <= 200.0 => 5
      case x if x <= 500.0 => 6
      case _ => 7
    }
    raw_list.append(sample.price.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}
