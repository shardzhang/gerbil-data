package featurizer.ml1m

import featurizer.ml1m.ML1MSample
import featurizer.core.{CategoricalFeature, RawFeature}

class ContextTimeHour(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    raw_list.append(sample.time_hour.toString)
    feature_list.append(sample.time_hour + 1)
    value_list.append(1.0F)
    this
  }
}

class ContextTimeArea(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    raw_list.append(sample.time_area.toString)
    feature_list.append(sample.time_area + 1)
    value_list.append(1.0F)
    this
  }
}

class ContextTimeWeek(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    raw_list.append(sample.week_day.toString)
    feature_list.append(sample.week_day)
    value_list.append(1.0F)
    this
  }
}

class IsWeekend(f_i: Int, f_n: String) extends CategoricalFeature[ML1MSample](f_i, f_n) {
  override def parse(sample: ML1MSample): RawFeature = {
    val w = sample.week_day
    val flag = if (w == 6 || w == 7) 2 else 1
    raw_list.append(flag.toString)
    feature_list.append(flag)
    value_list.append(1.0F)
    this
  }
}
