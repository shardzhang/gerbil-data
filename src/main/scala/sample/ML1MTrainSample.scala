package sample


import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.collection.mutable
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import com.alibaba.fastjson.JSON

import utils.LogUtils.green_println

/**
 * @author shard zhang
 * @date 2026/5/29 18:02
 * @note 
 *  
 *  ML-1M 训练样本, 包含用户特征, 物品特征, 用户行为序列特征, 标签
 * 
 *       标签: rating
 *       物品特征: movie_title, movie_genres, movie_publish_year, movie_rate_count, movie_avg_rate
 *       用户特征: gender, age, occupation, zip_code
 *       用户统计特征： user_rate_count, user_avg_rate
 *       上下文特征: time_hour, time_area, week_day
 *       用户行为序列特征: user_movie_rate, user_movie_rate_1days, user_movie_rate_3days, user_movie_rate_7days, user_movie_rate_15days
 */
class ML1MTrainSample extends Serializable {
  override def toString: String = {
    val str = new StringBuilder()

    // user
    str.append(s"user_id: ${user_id}\n")
    str.append(s"gender: ${gender}\n")
    str.append(s"age: ${age}\n")
    str.append(s"occupation: ${occupation}\n")
    str.append(s"zip_code: ${zip_code}\n")

    // item
    str.append(s"item_id: ${item_id}\n")
    str.append(s"title: ${movie_title}\n")
    str.append(s"rate_count: ${movie_rate_count}\n")
    str.append(s"avg_rate: ${movie_avg_rate}\n")

    // context
    str.append(s"time_hour: ${time_hour}\n")
    str.append(s"time_area: ${time_area}\n")
    str.append(s"week_day: ${week_day}\n")

    // user_behavior
    str.append(s"user_movie_rate: ${user_movie_rates.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_movie_rate_1days: ${user_movie_rate_1days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_movie_rate_3days: ${user_movie_rate_3days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_movie_rate_7days: ${user_movie_rate_7days.map(s => s._1 + ":" + s._2).mkString(",")}\n")
    str.append(s"user_movie_rate_15days: ${user_movie_rate_15days.map(s => s._1 + ":" + s._2).mkString(",")}\n")

    // target
    str.append(s"target: ${target}\n")
    str.append(s"rating: ${rating}\n")

    str.toString()
  }

  /** ***************************** user *********************************** */
  // 1 - 6040
  var user_id: String = ""

  // M / F
  var gender: String = ""

  // 1 → 0-18
  // 18 → 18-24
  // 25 → 25-34
  // 35 → 35-44
  // 45 → 45-49
  // 50 → 50-55
  var age: String = ""

  // 0 → 其他 / 未说明
  // 1 → 学术 / 教育工作者
  // 2 → 艺术家
  // 3 → 文书 / 行政
  // 4 → 大学生 / 研究生
  // 5 → 客户服务
  // 6 → 医生 / 医疗保健
  // 7 → 高管 / 管理
  // 8 → 农民
  // 9 → 家庭主妇
  // 10 → K-12 学生
  // 11 → 律师
  // 12 → 程序员
  // 13 → 退休
  // 14 → 销售 / 营销
  // 15 → 科学家
  // 16 → 自雇
  // 17 → 技术人员 / 工程师
  // 18 → 工匠 / 技工
  // 19 → 失业
  // 20 → 作家
  var occupation: String = ""
  // 邮政编码
  var zip_code: String = ""

  /** ***************************** item *********************************** */
  // 1-3952
  var item_id: String = ""
  // 电影标题
  var movie_title: String = ""
  // 电影发布年份
  var movie_publish_year: Int = 0
  // 电影类型. Action、Adventure、Animation、Children's、Comedy、Crime、Documentary、Drama、Fantasy、Film-Noir、Horror、Musical、Mystery、Romance、Sci-Fi、Thriller、War、Western
  var movie_genres: ArrayBuffer[String] = ArrayBuffer.empty[String]

  // item Statistical
  // 电影评分人数
  var movie_rate_count: Long = 0
  // 电影均分
  var movie_avg_rate: Double = 0
  // 电影热度排名
  var movie_hot_rank: Int = 99999
  // 电影类型数量
  var movie_genre_cnt: Int = 0

  /** ***************************** user behavior *********************************** */
  // 用户对电影评分序列 (电影ID, 评分)
  var user_movie_rates: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_1days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_3days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_7days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_15days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]

  // 用户评分次数总次数
  var user_rate_cnt: Int = 0
  // 用户评分次数7day
  var user_rate_7day_cnt: Int = 0
  // 用户评分次数15day
  var user_rate_15day_cnt: Int = 0
  // 用户评分次数30day
  var user_rate_30day_cnt: Int = 0

  // 用户打分方差
  var user_rate_std: Float = 0.0F
  // 用户打分方差7天
  var user_rate_std_7day: Float = 0.0F
  // 用户打分方差15天
  var user_rate_std_15day: Float = 0.0F
  // 用户打分方差30天
  var user_rate_std_30day: Float = 0.0F

  // 用户平均评分. 低分用户 (＜3)、中庸3、高分偏好 (≥4)
  var user_avg_rate: Float = 3.0F
  var user_avg_rate_7day: Float = 3.0F
  var user_avg_rate_15day: Float = 3.0F
  var user_avg_rate_30day: Float = 3.0F

  // 用户对电影类型评分序列 (电影类型, 给类型下所有电影的平均评分)
  var user_genres_rates: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_1days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_3days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_7days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_15days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]

  // 用户对电影类型评分次数序列 (电影类型, 给类型下所有电影评分总次数)
  var user_genres_rate_cnts: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_1days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_3days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_7days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_15days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]

  // 用户历史最爱top3电影类型和对应评分次数
  var user_top3_genres: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]

  // liftcycle生命周期
  // 用户活跃天数 (累计有评分的去重天数)
  var user_active_day: Int = 0
  // 用户注册至今天数（生命周期）
  var user_reg_day: Int = 0
  // 最后一次行为距今天数（沉默天数）
  var user_last_behavior_day: Int = 0
  // 电影发布距今天数/年份差
  var item_publish_day: Int = 0


  /** ***************************** context *********************************** */
  // 1-7
  var week_day: Int = 0
  // 0-23
  var time_hour: Int = 0
  // 0-5
  var time_area: Int = 0

  /** ***************************** target *********************************** */
  // 多分类. item_id.
  var target: Int = 0
  // 二分类. 0/1
  var label: Int = 0
  // 相关性回归. 1-5
  var rating: Float = 0.0F
}

object ML1MTrainSample {
  /** The seed to be used, copied from scala's murmurhash implementation. */
  final val SEED: Int = 0x3c074a61

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  private def parseTimestampMillis(raw: String): Long = {
    val trimmed = raw.trim
    if (trimmed.forall(_.isDigit)) {
      val value = trimmed.toLong
      if (trimmed.length <= 10) value * 1000L else value
    } else {
      LocalDateTime.parse(trimmed, formatter).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    }
  }

  private def parseUserAvgRate(user_movie_rates: ArrayBuffer[(Int, Int)]) = {
    val total_rate = user_movie_rates.map(r => r._2).sum
    val total_cnt = user_movie_rates.length
    total_rate / total_cnt
  }

  /**
   * Parse join_sample
   *
   * @param row        ("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "item_feature", "user_behavior"))
   * @param movie_info Map[movie_id, (title, genres)]
   * @return
   */
  def parseSample(row: Row, movie_info: immutable.Map[Int, (String, Array[String])]): (ML1MTrainSample, Boolean) = {
    val train_sample = new ML1MTrainSample()
    var ret = true

    // parse target
    train_sample.target =
      try {
        row.getAs[String]("item_id").toInt
      } catch {
        case e: Exception =>
          green_println(s"train_sample.target parse error: ${e.getMessage}, row=${row.toSeq}")
          ret = false
          0
      }

    // parse user_id
    train_sample.user_id =
      try {
        row.getAs[String]("user_id")
      } catch {
        case e: Exception =>
          green_println(s"train_sample.user_id parse error: ${e.getMessage}, row=${row.toSeq}")
          ret = false
          ""
      }

    // parse item_id
    train_sample.item_id =
      try {
        row.getAs[String]("item_id")
      } catch {
        case e: Exception =>
          green_println(s"train_sample.item_id parse error: ${e.getMessage}, row=${row.toSeq}")
          ret = false
          ""
      }

    // parse rating
    train_sample.rating =
      try {
        row.getAs[String]("rating").toInt
      } catch {
        case e: Exception =>
          green_println(s"train_sample.rating parse error: ${e.getMessage}, row=${row.toSeq}")
          ret = false
          0
      }
    train_sample.label = if (train_sample.rating >= 3) 1 else 0

    // parse context
    var sample_timestamp = 0L
    try {
      val timeStr = row.getAs[String]("time_stamp")
      val ts = parseTimestampMillis(timeStr)
      val dt = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(ts), ZoneId.systemDefault())
      train_sample.time_hour = dt.getHour
      train_sample.time_area = dt.getHour / 4
      train_sample.week_day = dt.getDayOfWeek.getValue
      sample_timestamp = ts
    } catch {
      case e: Exception => {
        green_println("train_sample.time_hour: " + e.toString + " " + row.toSeq.toString())
        ret = false
      }
    }

    // parse item_feature
    var json = row.getAs[String]("movie_feature")
    if (json != null && json != "{}") {
      val item_feature = JSON.parseObject(json)
      train_sample.movie_title = item_feature.getString("movie_title")
      train_sample.movie_publish_year = try {
        val pattern = "\\((\\d{4})\\)".r
        pattern.findFirstMatchIn(train_sample.movie_title).map(_.group(1).toInt).getOrElse(1990)
      } catch {
        case _: Exception => 0
      }
      val genres = item_feature.getString("movie_genres")
      if (genres != null) {
        train_sample.movie_genres.appendAll(genres.split("\\|").map(r => r.trim.toLowerCase()))
        train_sample.movie_genre_cnt = train_sample.movie_genres.size
      }
      train_sample.movie_rate_count = item_feature.getLong("movie_rate_count")
      train_sample.movie_avg_rate = item_feature.getDouble("movie_avg_rate")
      train_sample.movie_hot_rank = item_feature.getIntValue("movie_hot_rank")
    }

    // parse user_profile
    json = row.getAs[String]("user_profile")
    if (json != null && json != "{}") {
      val user_profile = JSON.parseObject(json)
      train_sample.gender = user_profile.getString("gender")
      train_sample.age = user_profile.getString("age")
      train_sample.occupation = user_profile.getString("occupation")
      train_sample.zip_code = user_profile.getString("zip_code")
    }

    // 解析用户统计特征
    json = try {
      row.getAs[String]("user_stat_feature")
    } catch {
      case _: Exception => null
    }
    if (json != null && json != "{}") {
      val user_stat_feature = JSON.parseObject(json)
      train_sample.user_active_day = user_stat_feature.getIntValue("active_days")
    }

    // parse user behavior
    json = row.getAs[String]("user_behavior")
    if (json != null && json != "{}") {
      val user_behavior = JSON.parseObject(json)
      // user_movie_rate
      val user_movie_rate_seq = user_behavior.getString("user_movie_rate")
      if (user_movie_rate_seq != null && user_movie_rate_seq.nonEmpty) {
        val user_genres_rate_map = mutable.Map[String, Float]().withDefaultValue(0.0f)
        val user_genres_rate_1day_map = mutable.Map[String, Float]().withDefaultValue(0.0f)
        val user_genres_rate_3day_map = mutable.Map[String, Float]().withDefaultValue(0.0f)
        val user_genres_rate_7day_map = mutable.Map[String, Float]().withDefaultValue(0.0f)
        val user_genres_rate_15day_map = mutable.Map[String, Float]().withDefaultValue(0.0f)
        val user_genres_rate_cnt_map = mutable.Map[String, Int]().withDefaultValue(0)
        val user_genres_rate_cnt_1day_map = mutable.Map[String, Int]().withDefaultValue(0)
        val user_genres_rate_cnt_3day_map = mutable.Map[String, Int]().withDefaultValue(0)
        val user_genres_rate_cnt_7day_map = mutable.Map[String, Int]().withDefaultValue(0)
        val user_genres_rate_cnt_15day_map = mutable.Map[String, Int]().withDefaultValue(0)
        for (item <- user_movie_rate_seq.split(",")) {
          val parts = item.split(":")
          if (parts.length >= 3) {
            val item_id = parts(0).toInt
            val rate = parts(1).toInt
            val timestamp = parts(2)
            var has_movie_info = true
            val (title, genres) = movie_info.get(parts(0).toInt) match {
              case Some(tg) => tg
              case None =>
                has_movie_info = false
                ("", Array.empty[String])
            }
            val dur = try {
              val ts = parseTimestampMillis(timestamp)
              (sample_timestamp - ts) / 1000.0 / 3600.0 / 24.0
            } catch {
              case e: Exception =>
                green_println("Parse user_movie_rate: " + e.toString + " " + item)
                ret = false
                Double.MaxValue
            }
            if (dur >= 0) {
              train_sample.user_movie_rates.append((item_id, rate))
              train_sample.user_rate_cnt += 1
              train_sample.user_avg_rate = parseUserAvgRate(train_sample.user_movie_rates)
            }
            if (dur >= 0 && dur <= 1) {
              train_sample.user_movie_rate_1days.append((item_id, rate))
              train_sample.user_avg_rate = parseUserAvgRate(train_sample.user_movie_rate_1days)
            }
            if (dur >= 0 && dur <= 3) {
              train_sample.user_movie_rate_3days.append((item_id, rate))
              train_sample.user_avg_rate = parseUserAvgRate(train_sample.user_movie_rate_3days)
            }
            if (dur >= 0 && dur <= 7) {
              train_sample.user_movie_rate_7days.append((item_id, rate))
              train_sample.user_rate_7day_cnt += 1
              train_sample.user_avg_rate = parseUserAvgRate(train_sample.user_movie_rate_7days)
            }
            if (dur >= 0 && dur <= 15) {
              train_sample.user_movie_rate_15days.append((item_id, rate))
              train_sample.user_rate_15day_cnt += 1
              train_sample.user_avg_rate = parseUserAvgRate(train_sample.user_movie_rate_15days)
            }
            if (dur >= 0 && dur <= 30) {
              train_sample.user_rate_30day_cnt += 1
            }

            for (g <- genres) {
              val gen = g.trim.toLowerCase()
              if (dur >= 0) {
                user_genres_rate_cnt_map(gen) += 1
                user_genres_rate_map(gen) += rate
              }
              if (dur >= 0 && dur <= 1) {
                user_genres_rate_cnt_1day_map(gen) += 1
                user_genres_rate_1day_map(gen) += rate
              }
              if (dur >= 0 && dur <= 3) {
                user_genres_rate_cnt_3day_map(gen) += 1
                user_genres_rate_3day_map(gen) += rate
              }
              if (dur >= 0 && dur <= 7) {
                user_genres_rate_cnt_7day_map(gen) += 1
                user_genres_rate_7day_map(gen) += rate
              }
              if (dur >= 0 && dur <= 15) {
                user_genres_rate_cnt_15day_map(gen) += 1
                user_genres_rate_15day_map(gen) += rate
              }
            }
          }
        }
        for ((gen, total_rate) <- user_genres_rate_map) {
          val total_cnt = user_genres_rate_cnt_map(gen)
          train_sample.user_genres_rates.append((gen, total_rate / total_cnt))
          train_sample.user_genres_rate_cnts.append((gen, total_cnt))
        }
        for ((gen, total_rate) <- user_genres_rate_1day_map) {
          val total_cnt = user_genres_rate_cnt_1day_map(gen)
          train_sample.user_genres_rate_1days.append((gen, total_rate / total_cnt))
          train_sample.user_genres_rate_cnt_1days.append((gen, total_cnt))
        }
        for ((gen, total_rate) <- user_genres_rate_3day_map) {
          val total_cnt = user_genres_rate_cnt_3day_map(gen)
          train_sample.user_genres_rate_3days.append((gen, total_rate / total_cnt))
          train_sample.user_genres_rate_cnt_3days.append((gen, total_cnt))
        }
        for ((gen, total_rate) <- user_genres_rate_7day_map) {
          val total_cnt = user_genres_rate_cnt_7day_map(gen)
          train_sample.user_genres_rate_7days.append((gen, total_rate / total_cnt))
          train_sample.user_genres_rate_cnt_7days.append((gen, total_cnt))
        }
        for ((gen, total_rate) <- user_genres_rate_15day_map) {
          val total_cnt = user_genres_rate_cnt_15day_map(gen)
          train_sample.user_genres_rate_15days.append((gen, total_rate / total_cnt))
          train_sample.user_genres_rate_cnt_15days.append((gen, total_cnt))
        }
        train_sample.user_top3_genres = train_sample.user_genres_rate_cnts.sortBy(-_._2).take(3)
      }
    }
    (train_sample, ret)
  }
}
