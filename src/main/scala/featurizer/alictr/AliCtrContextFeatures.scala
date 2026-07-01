package featurizer.alictr

import featurizer.core.{CategoricalFeature, RawFeature}

class AliCtrPid(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    if (sample.pid != null && sample.pid.nonEmpty) {
      val p = new utils.MurmurHash3.LongPair()
      utils.MurmurHash3.murmurhash3_x64_128(sample.pid.getBytes, 0, sample.pid.length, SEED, p)
      raw_list.append(sample.pid)
      feature_list.append(p.val1)
      value_list.append(1.0F)
    }
    this
  }
}

class AliCtrTimeHour(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    raw_list.append(sample.time_hour.toString)
    feature_list.append(sample.time_hour + 1)
    value_list.append(1.0F)
    this
  }
}

class AliCtrTimeArea(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    raw_list.append(sample.time_area.toString)
    feature_list.append(sample.time_area + 1)
    value_list.append(1.0F)
    this
  }
}

class AliCtrWeekDay(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    raw_list.append(sample.week_day.toString)
    feature_list.append(sample.week_day)
    value_list.append(1.0F)
    this
  }
}
