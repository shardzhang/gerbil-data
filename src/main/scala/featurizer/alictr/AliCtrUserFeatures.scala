package featurizer.alictr

import featurizer.core.{CategoricalFeature, ContinuousFeature, RawFeature}
import utils.MurmurHash3

class AliCtrUserID(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val p = new MurmurHash3.LongPair()
    MurmurHash3.murmurhash3_x64_128(sample.user.getBytes, 0, sample.user.length, SEED, p)
    raw_list.append(sample.user)
    feature_list.append(p.val1)
    value_list.append(1.0F)
    this
  }
}

class AliCtrCmsSegID(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    raw_list.append(sample.cms_segid)
    feature_list.append(try { sample.cms_segid.toLong } catch { case _: Exception => 0L })
    value_list.append(1.0F)
    this
  }
}

class AliCtrCmsGroupID(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    raw_list.append(sample.cms_group_id)
    feature_list.append(try { sample.cms_group_id.toLong } catch { case _: Exception => 0L })
    value_list.append(1.0F)
    this
  }
}

class AliCtrGender(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val buck = sample.gender match {
      case "1" => 1
      case "2" => 2
      case _ => 0
    }
    raw_list.append(sample.gender)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class AliCtrAgeLevel(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val v = try { sample.age_level.toLong } catch { case _: Exception => 0L }
    raw_list.append(sample.age_level)
    feature_list.append(v)
    value_list.append(1.0F)
    this
  }
}

class AliCtrPvalueLevel(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val v = try { sample.pvalue_level.toLong } catch { case _: Exception => 0L }
    raw_list.append(sample.pvalue_level)
    feature_list.append(v)
    value_list.append(1.0F)
    this
  }
}

class AliCtrShoppingLevel(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val v = try { sample.shopping_level.toLong } catch { case _: Exception => 0L }
    raw_list.append(sample.shopping_level)
    feature_list.append(v)
    value_list.append(1.0F)
    this
  }
}

class AliCtrOccupation(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val v = try { sample.occupation.toLong } catch { case _: Exception => 0L }
    raw_list.append(sample.occupation)
    feature_list.append(v)
    value_list.append(1.0F)
    this
  }
}

class AliCtrNewUserClass(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val v = try { sample.new_user_class_level.toLong } catch { case _: Exception => 0L }
    raw_list.append(sample.new_user_class_level)
    feature_list.append(v)
    value_list.append(1.0F)
    this
  }
}

class AliCtrUserHistoryAdCnt(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val buck = sample.user_history_cnt match {
      case x if x == 0 => 1
      case x if x <= 5 => 2
      case x if x <= 20 => 3
      case x if x <= 50 => 4
      case x if x <= 100 => 5
      case _ => 6
    }
    raw_list.append(sample.user_history_cnt.toString)
    feature_list.append(buck)
    value_list.append(1.0F)
    this
  }
}

class AliCtrUserHistoryClkRate(f_i: Int, f_n: String) extends ContinuousFeature[AliCtrSample](f_i, f_n) {
  override def parse(sample: AliCtrSample): RawFeature = {
    val rate = if (sample.user_history_cnt > 0) {
      sample.user_history_clk.toFloat / sample.user_history_cnt
    } else 0.0F
    raw_list.append(rate.toString)
    feature_list.append(1L)
    value_list.append(rate)
    this
  }
}
