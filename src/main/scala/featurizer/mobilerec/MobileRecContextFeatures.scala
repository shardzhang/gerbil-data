package featurizer.mobilerec

import featurizer.core.{CategoricalFeature, RawFeature}

class MobileRecTimeHour(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    raw_list.append(sample.time_hour.toString)
    feature_list.append(sample.time_hour + 1)
    value_list.append(1.0F)
    this
  }
}

class MobileRecTimeArea(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    raw_list.append(sample.time_area.toString)
    feature_list.append(sample.time_area + 1)
    value_list.append(1.0F)
    this
  }
}

class MobileRecWeekDay(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    raw_list.append(sample.week_day.toString)
    feature_list.append(sample.week_day)
    value_list.append(1.0F)
    this
  }
}

class MobileRecIsWeekend(f_i: Int, f_n: String) extends CategoricalFeature[MobileRecSample](f_i, f_n) {
  override def parse(sample: MobileRecSample): RawFeature = {
    val w = sample.week_day
    val flag = if (w == 6 || w == 7) 2 else 1
    raw_list.append(flag.toString)
    feature_list.append(flag)
    value_list.append(1.0F)
    this
  }
}
