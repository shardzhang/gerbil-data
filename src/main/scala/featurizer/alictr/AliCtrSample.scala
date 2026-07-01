package featurizer.alictr

import scala.collection.mutable.ArrayBuffer

class AliCtrSample extends Serializable {
  var user: String = ""
  var adgroup_id: String = ""
  var clk: Int = 0
  var label: Int = 0
  var target: Int = 0
  var time_stamp: Long = 0L
  var pid: String = ""

  var cate_id: String = ""
  var campaign_id: String = ""
  var customer: String = ""
  var brand: String = ""
  var price: Double = 0.0

  var cms_segid: String = ""
  var cms_group_id: String = ""
  var gender: String = ""
  var age_level: String = ""
  var pvalue_level: String = ""
  var shopping_level: String = ""
  var occupation: String = ""
  var new_user_class_level: String = ""

  var time_hour: Int = 0
  var time_area: Int = 0
  var week_day: Int = 0

  var user_history_ads: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_history_cnt: Int = 0
  var user_history_clk: Int = 0
}

object AliCtrSample {
  def parseSample(user: String, adgroupId: String, clk: Int,
                  timestamp: Long, pid: String,
                  cateId: String, campaignId: String, customer: String,
                  brand: String, price: Double,
                  cmsSegid: String, cmsGroupId: String, gender: String,
                  ageLevel: String, pvalueLevel: String, shoppingLevel: String,
                  occupation: String, newUserClassLevel: String,
                  userHistorySeq: String): (AliCtrSample, Boolean) = {
    val s = new AliCtrSample()
    var ret = true

    s.user = user
    s.adgroup_id = adgroupId
    s.clk = clk
    s.label = clk
    s.target = try { adgroupId.hashCode } catch { case _: Exception => 0 }
    s.time_stamp = timestamp
    s.pid = pid

    s.cate_id = cateId
    s.campaign_id = campaignId
    s.customer = customer
    s.brand = brand
    s.price = price

    s.cms_segid = cmsSegid
    s.cms_group_id = cmsGroupId
    s.gender = gender
    s.age_level = ageLevel
    s.pvalue_level = pvalueLevel
    s.shopping_level = shoppingLevel
    s.occupation = occupation
    s.new_user_class_level = newUserClassLevel

    val ts = if (timestamp > 0) timestamp * 1000L else System.currentTimeMillis()
    try {
      val dt = java.time.LocalDateTime.ofInstant(
        java.time.Instant.ofEpochMilli(ts),
        java.time.ZoneId.systemDefault())
      s.time_hour = dt.getHour
      s.time_area = dt.getHour / 4
      s.week_day = dt.getDayOfWeek.getValue
    } catch {
      case _: Exception => ret = false
    }

    if (userHistorySeq != null && userHistorySeq.nonEmpty) {
      for (item <- userHistorySeq.split(",")) {
        val parts = item.split(":")
        if (parts.length >= 2) {
          s.user_history_ads.append((parts(0), parts(1).toInt))
          s.user_history_clk += parts(1).toInt
        }
      }
      s.user_history_cnt = s.user_history_ads.length
    }

    (s, ret)
  }
}
