package sample

import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import utils.LogUtils.green_println
import scala.jdk.CollectionConverters._
import com.alibaba.fastjson.JSON
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

/**
 * @author shard zhang
 * @date 2026/5/29 18:02
 * @note ML-1M
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
    str.append(s"title: ${title}\n")
    str.append(s"rate_count: ${rate_count}\n")
    str.append(s"avg_rate: ${avg_rate}\n")

    // context
    str.append(s"time_hour: ${time_hour}\n")
    str.append(s"time_area: ${time_area}\n")
    str.append(s"week_day: ${week_day}\n")

    // user_behavior
    str.append(s"user_movie_rate: ${user_movie_rate.map(s => s._1 + ":" + s._2).mkString(",")}\n")
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
  var user_id: String = ""
  var gender: String = ""
  var age: String = ""
  var occupation: String = ""
  var zip_code: String = ""

  /** ***************************** item *********************************** */
  var item_id: String = ""
  var title: String = ""
  var genres: ListBuffer[String] = ListBuffer[String]()
  var rate_count: Long = 0
  var avg_rate: Double = 0

  /** ***************************** user behavior *********************************** */
  var user_movie_rate: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_movie_rate_1days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_movie_rate_3days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_movie_rate_7days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_movie_rate_15days: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]
  var user_movie_rate_invalid: ListBuffer[(Int, Int)] = new ListBuffer[(Int, Int)]

  /** ***************************** context *********************************** */
  var week_day: Int = 0
  var time_hour: Int = 0
  var time_area: Int = 0

  /** ***************************** target *********************************** */
  var target: Int = 0
  var rating: Int = 0
}

object ML1MTrainSample {
  /** The seed to be used, copied from scala's murmurhash implementation. */
  final val SEED: Int = 0x3c074a61

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /**
   * Parse join_sample
   *
   * @param row ("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "item_feature", "user_behavior"))
   * @return
   */
  def parseSample(row: Row): (ML1MTrainSample, Boolean) = {
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

    // parse context
    var sample_timestamp = 0L
    try {
      val timeStr = row.getAs[String]("time_stamp")
      val dt = LocalDateTime.parse(timeStr, formatter)
      train_sample.time_hour = dt.getHour
      train_sample.time_area = dt.getHour / 4
      train_sample.week_day = dt.getDayOfWeek.getValue
      sample_timestamp = dt.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    } catch {
      case e: Exception => {
        green_println("train_sample.time_hour: " + e.toString + " " + row.toSeq.toString())
        ret = false
      }
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

    // parse item_feature
    var json = row.getAs[String]("item_feature")
    if (json != null && json != "{}") {
      val item_feature = JSON.parseObject(json)
      train_sample.title = item_feature.getString("title")
      val genresArray = item_feature.getJSONArray("genres")
      if (genresArray != null) {
        train_sample.genres ++= genresArray.toJavaList(classOf[String]).asScala
      }
      train_sample.rate_count = item_feature.getLong("rate_count")
      train_sample.avg_rate = item_feature.getDouble("avg_rate")
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

    // parse user behavior
    json = row.getAs[String]("user_behavior")
    if (json != null && json != "{}") {
      val user_behavior = JSON.parseObject(json)

      // user_movie_rate
      val user_movie_rate_seq = user_behavior.getString("user_movie_rate")
      if (user_movie_rate_seq != null && user_movie_rate_seq.nonEmpty) {
        val user_movie_rate_items = user_movie_rate_seq.split(",")
        for (item <- user_movie_rate_items) {
          val parts = item.split(":")
          if (parts.length >= 3) {
            val item_id = parts(0).toInt
            val cnt = parts(1).toInt
            val timestamp = parts(2)

            val dur = try {
              val dt = LocalDateTime.parse(timestamp, formatter)
              val ts = dt.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
              (sample_timestamp - ts) / 1000.0 / 3600.0 / 24.0
            } catch {
              case e: Exception =>
                green_println("Parse user_movie_rate: " + e.toString + " " + item)
                ret = false
                Double.MaxValue
            }
            green_println(s"user_movie_rate. item_id: ${item_id}, cnt: ${cnt}, timestamp: ${timestamp}, dur: ${dur}")

            if (dur >= 0) {
              train_sample.user_movie_rate.append((item_id, cnt))
            }
            if (dur >= 0 && dur <= 1) {
              train_sample.user_movie_rate_1days.append((item_id, cnt))
            }
            if (dur >= 0 && dur <= 3) {
              train_sample.user_movie_rate_3days.append((item_id, cnt))
            }
            if (dur >= 0 && dur <= 7) {
              train_sample.user_movie_rate_7days.append((item_id, cnt))
            }
            if (dur >= 0 && dur <= 15) {
              train_sample.user_movie_rate_15days.append((item_id, cnt))
            }
          }
        }
      }
    }
    (train_sample, ret)
  }
}
