package featurizer.mobilerec

import scala.collection.mutable.ArrayBuffer

class MobileRecSample extends Serializable {
  var uid: String = ""
  var app_package: String = ""
  var rating: Float = 0.0F
  var label: Int = 0
  var target: Int = 0
  var unix_timestamp: Long = 0L
  var app_category: String = ""
  var review: String = ""

  var app_price: Double = 0.0
  var app_avg_rating: Double = 0.0
  var app_num_reviews: Long = 0L
  var app_content_rating: String = ""

  var user_apps_rates: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_apps_rate_1days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_apps_rate_3days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_apps_rate_7days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_apps_rate_15days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_apps_rate_30days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty

  var user_rate_cnt: Int = 0
  var user_rate_7day_cnt: Int = 0
  var user_rate_15day_cnt: Int = 0
  var user_rate_30day_cnt: Int = 0

  var user_rate_std: Float = 0.0F
  var user_rate_std_7day: Float = 0.0F
  var user_rate_std_15day: Float = 0.0F
  var user_rate_std_30day: Float = 0.0F

  var user_avg_rate: Float = 3.0F
  var user_avg_rate_7day: Float = 3.0F
  var user_avg_rate_15day: Float = 3.0F
  var user_avg_rate_30day: Float = 3.0F

  var user_categories_rates: ArrayBuffer[(String, Float)] = ArrayBuffer.empty
  var user_categories_rate_1days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty
  var user_categories_rate_3days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty
  var user_categories_rate_7days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty
  var user_categories_rate_15days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty
  var user_categories_rate_30days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty

  var user_categories_rate_cnts: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_categories_rate_cnt_1days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_categories_rate_cnt_3days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_categories_rate_cnt_7days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_categories_rate_cnt_15days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_categories_rate_cnt_30days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty

  var user_top3_categories: ArrayBuffer[(String, Int)] = ArrayBuffer.empty
  var user_active_day: Int = 0

  var time_hour: Int = 0
  var time_area: Int = 0
  var week_day: Int = 0
  var time_stamp: Long = 0L
}

object MobileRecSample {
  def parseSample(uid: String, appPackage: String, rating: Float,
                  timestamp: Long, appCategory: String, review: String): (MobileRecSample, Boolean) = {
    val s = new MobileRecSample()
    var ret = true

    s.uid = uid
    s.app_package = appPackage
    s.rating = rating
    s.target = try { appPackage.hashCode } catch { case _: Exception => 0 }
    s.label = if (rating > 3) 1 else 0
    s.unix_timestamp = timestamp
    s.app_category = appCategory
    s.review = review

    val ts = if (timestamp > 0) timestamp * 1000L else System.currentTimeMillis()
    try {
      val dt = java.time.LocalDateTime.ofInstant(
        java.time.Instant.ofEpochMilli(ts),
        java.time.ZoneId.systemDefault())
      s.time_hour = dt.getHour
      s.time_area = dt.getHour / 4
      s.week_day = dt.getDayOfWeek.getValue
      s.time_stamp = ts
    } catch {
      case _: Exception => ret = false
    }
    (s, ret)
  }

  def parseSampleFromLine(userId: String, itemId: String, rating: Float,
                           timeStamp: Long, appCategory: String, review: String,
                           userAppRateSeq: String): (MobileRecSample, Boolean) = {
    val (s, ret) = parseSample(userId, itemId, rating, timeStamp, appCategory, review)

    if (userAppRateSeq != null && userAppRateSeq.nonEmpty) {
      val userAppsRate30Days = scala.collection.mutable.ArrayBuffer.empty[(String, Int)]
      val userCategoriesRateMap = scala.collection.mutable.Map[String, Float]().withDefaultValue(0.0f)
      val userCategoriesRate1dayMap = scala.collection.mutable.Map[String, Float]().withDefaultValue(0.0f)
      val userCategoriesRate3dayMap = scala.collection.mutable.Map[String, Float]().withDefaultValue(0.0f)
      val userCategoriesRate7dayMap = scala.collection.mutable.Map[String, Float]().withDefaultValue(0.0f)
      val userCategoriesRate15dayMap = scala.collection.mutable.Map[String, Float]().withDefaultValue(0.0f)
      val userCategoriesRate30dayMap = scala.collection.mutable.Map[String, Float]().withDefaultValue(0.0f)
      val userCategoriesRateCntMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
      val userCategoriesRateCnt1dayMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
      val userCategoriesRateCnt3dayMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
      val userCategoriesRateCnt7dayMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
      val userCategoriesRateCnt15dayMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
      val userCategoriesRateCnt30dayMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)

      for (item <- userAppRateSeq.split(",")) {
        val parts = item.split(":")
        if (parts.length >= 3) {
          val appId = parts(0)
          val rate = parts(1).toInt
          val itemTs = parts(2).toLong
          val dur = if (s.time_stamp > 0 && itemTs > 0) {
            (s.time_stamp - itemTs * 1000L) / 1000.0 / 3600.0 / 24.0
          } else -1.0

          if (dur > 0) {
            s.user_apps_rates.append((appId, rate))
            s.user_rate_cnt += 1
          }
          if (dur > 0 && dur <= 1) s.user_apps_rate_1days.append((appId, rate))
          if (dur > 0 && dur <= 3) s.user_apps_rate_3days.append((appId, rate))
          if (dur > 0 && dur <= 7) {
            s.user_apps_rate_7days.append((appId, rate))
            s.user_rate_7day_cnt += 1
          }
          if (dur > 0 && dur <= 15) {
            s.user_apps_rate_15days.append((appId, rate))
            s.user_rate_15day_cnt += 1
          }
          if (dur > 0 && dur <= 30) {
            userAppsRate30Days.append((appId, rate))
            s.user_rate_30day_cnt += 1
          }

          if (dur > 0) {
            userCategoriesRateCntMap(appCategory) += 1
            userCategoriesRateMap(appCategory) += rate
          }
          if (dur > 0 && dur <= 1) {
            userCategoriesRateCnt1dayMap(appCategory) += 1
            userCategoriesRate1dayMap(appCategory) += rate
          }
          if (dur > 0 && dur <= 3) {
            userCategoriesRateCnt3dayMap(appCategory) += 1
            userCategoriesRate3dayMap(appCategory) += rate
          }
          if (dur > 0 && dur <= 7) {
            userCategoriesRateCnt7dayMap(appCategory) += 1
            userCategoriesRate7dayMap(appCategory) += rate
          }
          if (dur > 0 && dur <= 15) {
            userCategoriesRateCnt15dayMap(appCategory) += 1
            userCategoriesRate15dayMap(appCategory) += rate
          }
          if (dur > 0 && dur <= 30) {
            userCategoriesRateCnt30dayMap(appCategory) += 1
            userCategoriesRate30dayMap(appCategory) += rate
          }
        }
      }

      s.user_avg_rate = parseUserAvgRate(s.user_apps_rates)
      s.user_rate_std = parseUserRateStd(s.user_apps_rates)
      s.user_avg_rate_7day = parseUserAvgRate(s.user_apps_rate_7days)
      s.user_rate_std_7day = parseUserRateStd(s.user_apps_rate_7days)
      s.user_avg_rate_15day = parseUserAvgRate(s.user_apps_rate_15days)
      s.user_rate_std_15day = parseUserRateStd(s.user_apps_rate_15days)
      s.user_avg_rate_30day = parseUserAvgRate(userAppsRate30Days)
      s.user_rate_std_30day = parseUserRateStd(userAppsRate30Days)

      for ((cat, totalRate) <- userCategoriesRateMap) {
        val totalCnt = userCategoriesRateCntMap(cat)
        s.user_categories_rates.append((cat, totalRate / totalCnt))
        s.user_categories_rate_cnts.append((cat, totalCnt))
      }
      for ((cat, totalRate) <- userCategoriesRate1dayMap) {
        val totalCnt = userCategoriesRateCnt1dayMap(cat)
        s.user_categories_rate_1days.append((cat, totalRate / totalCnt))
        s.user_categories_rate_cnt_1days.append((cat, totalCnt))
      }
      for ((cat, totalRate) <- userCategoriesRate3dayMap) {
        val totalCnt = userCategoriesRateCnt3dayMap(cat)
        s.user_categories_rate_3days.append((cat, totalRate / totalCnt))
        s.user_categories_rate_cnt_3days.append((cat, totalCnt))
      }
      for ((cat, totalRate) <- userCategoriesRate7dayMap) {
        val totalCnt = userCategoriesRateCnt7dayMap(cat)
        s.user_categories_rate_7days.append((cat, totalRate / totalCnt))
        s.user_categories_rate_cnt_7days.append((cat, totalCnt))
      }
      for ((cat, totalRate) <- userCategoriesRate15dayMap) {
        val totalCnt = userCategoriesRateCnt15dayMap(cat)
        s.user_categories_rate_15days.append((cat, totalRate / totalCnt))
        s.user_categories_rate_cnt_15days.append((cat, totalCnt))
      }
      for ((cat, totalRate) <- userCategoriesRate30dayMap) {
        val totalCnt = userCategoriesRateCnt30dayMap(cat)
        s.user_categories_rate_30days.append((cat, totalRate / totalCnt))
        s.user_categories_rate_cnt_30days.append((cat, totalCnt))
      }
      s.user_top3_categories = s.user_categories_rate_cnts.sortBy(-_._2).take(3)
      s.user_active_day = s.user_apps_rates.length
    }
    (s, ret)
  }

  private def parseUserAvgRate(seq: scala.collection.mutable.ArrayBuffer[(String, Int)]): Float = {
    if (seq.isEmpty) return 3.0F
    seq.map(_._2).sum.toFloat / seq.length
  }

  private def parseUserRateStd(seq: scala.collection.mutable.ArrayBuffer[(String, Int)]): Float = {
    if (seq.isEmpty) return 0.0F
    val avg = parseUserAvgRate(seq)
    val variance = seq.map { case (_, rate) =>
      val diff = rate.toFloat - avg
      diff * diff
    }.sum / seq.length.toFloat
    math.sqrt(variance.toDouble).toFloat
  }
}
