package featurizer.ml1m

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import com.alibaba.fastjson.JSON

import utils.LogUtils.green_println

/**
 *
 *  ML-1M training sample, including user features, item features, user behavior sequence features, label
 *  Training sample with user/item/context features, behavior sequences, and target label.
 *
 *       Label: rating
 *       Item features: movie_title, movie_genres, movie_publish_year, movie_rate_count, movie_avg_rate
 *       User features: gender, age, occupation, zip_code
 *       User statistical features: user_rate_count, user_avg_rate
 *       Context features: time_hour, time_area, week_day
 *       User behavior sequence features: user_movie_rate, user_movie_rate_1days, user_movie_rate_3days, user_movie_rate_7days, user_movie_rate_15days
 */
class ML1MSample extends Serializable {
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

  // 0 → Other / Not specified
  // 1 → Academic / Educator
  // 2 → Artist
  // 3 → Clerical / Administrative
  // 4 → College / Graduate student
  // 5 → Customer service
  // 6 → Doctor / Healthcare
  // 7 → Executive / Management
  // 8 → Farmer
  // 9 → Homemaker
  // 10 → K-12 student
  // 11 → Lawyer
  // 12 → Programmer
  // 13 → Retired
  // 14 → Sales / Marketing
  // 15 → Scientist
  // 16 → Self-employed
  // 17 → Technician / Engineer
  // 18 → Craftsman / Mechanic
  // 19 → Unemployed
  // 20 → Writer
  var occupation: String = ""
  // Zip code
  var zip_code: String = ""

  /** ***************************** item *********************************** */
  // 1-3952
  var item_id: String = ""
  // Movie title
  var movie_title: String = ""
  // Movie publish year
  var movie_publish_year: Int = 0
  // Movie genres. Action, Adventure, Animation, Children's, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western
  var movie_genres: ArrayBuffer[String] = ArrayBuffer.empty[String]

  // item Statistical
  // Number of movie raters
  var movie_rate_count: Long = 0
  // Average movie rating
  var movie_avg_rate: Double = 0
  // Movie popularity rank
  var movie_hot_rank: Int = 99999
  // Movie genre count
  var movie_genre_cnt: Int = 0

  /** ***************************** user behavior *********************************** */
  // User rating sequence for movies (movie ID, rating)
  var user_movie_rates: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_1days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_3days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_7days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]
  var user_movie_rate_15days: ArrayBuffer[(Int, Int)] = ArrayBuffer.empty[(Int, Int)]

  // Total user rating count
  var user_rate_cnt: Int = 0
  // User rating count 7-day
  var user_rate_7day_cnt: Int = 0
  // User rating count 15-day
  var user_rate_15day_cnt: Int = 0
  // User rating count 30-day
  var user_rate_30day_cnt: Int = 0

  // User rating variance
  var user_rate_std: Float = 0.0F
  // User rating variance 7-day
  var user_rate_std_7day: Float = 0.0F
  // User rating variance 15-day
  var user_rate_std_15day: Float = 0.0F
  // User rating variance 30-day
  var user_rate_std_30day: Float = 0.0F

  // User average rating. Low-score users (<3), neutral 3, high-score preference (≥4)
  var user_avg_rate: Float = 3.0F
  var user_avg_rate_7day: Float = 3.0F
  var user_avg_rate_15day: Float = 3.0F
  var user_avg_rate_30day: Float = 3.0F

  // User rating sequence by movie genre (genre, average rating of all movies in that genre)
  var user_genres_rates: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_1days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_3days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_7days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]
  var user_genres_rate_15days: ArrayBuffer[(String, Float)] = ArrayBuffer.empty[(String, Float)]

  // User rating count sequence by movie genre (genre, total rating count for all movies in that genre)
  var user_genres_rate_cnts: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_1days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_3days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_7days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]
  var user_genres_rate_cnt_15days: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]

  // User's historical top 3 favorite movie genres and corresponding rating counts
  var user_top3_genres: ArrayBuffer[(String, Int)] = ArrayBuffer.empty[(String, Int)]

  // Lifecycle period
  // User active days (cumulative distinct days with ratings)
  var user_active_day: Int = 0
  // Days since user registration (lifecycle)
  var user_reg_day: Int = 0
  // Days since last behavior (silent days)
  var user_last_behavior_day: Int = 0
  // Days/years since movie release
  var item_publish_day: Int = 0


  /** ***************************** context *********************************** */
  // 1-7
  var week_day: Int = 0
  // 0-23
  var time_hour: Int = 0
  // 0-5
  var time_area: Int = 0
  // Unix timestamp in milliseconds (for time-based split)
  var time_stamp: Long = 0L

  /** ***************************** target *********************************** */
  // Multi-class classification. item_id.
  var target: Int = 0
  // Binary classification. 0/1
  var label: Int = 0
  // Relevance regression. 1-5
  var rating: Float = 0.0F
}

object ML1MSample {
  /** The seed to be used, copied from scala's murmurhash implementation. */
  final val SEED: Int = 0x3c074a61

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /** Parse timestamp: auto-detect unix seconds (<=10 digits) vs milliseconds vs formatted string (yyyyMMddHHmmss). */
  private def parseTimestampMillis(raw: String): Long = {
    val trimmed = raw.trim
    if (trimmed.forall(_.isDigit)) {
      val value = trimmed.toLong
      if (trimmed.length <= 10) value * 1000L else value
    } else {
      LocalDateTime.parse(trimmed, formatter).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    }
  }

  /** Compute average rating from a sequence of (movieId, rating) pairs. Returns 3.0 if empty. */
  private def parseUserAvgRate(user_movie_rates: ArrayBuffer[(Int, Int)]): Float = {
    if (user_movie_rates.isEmpty) {
      return 3.0F
    }
    val total_rate = user_movie_rates.map(r => r._2).sum
    val total_cnt = user_movie_rates.length
    total_rate * 1.0F / total_cnt
  }

  /** Compute rating standard deviation. Returns 0.0 if empty. */
  private def parseUserRateStd(user_movie_rates: ArrayBuffer[(Int, Int)]): Float = {
    if (user_movie_rates.isEmpty) {
      return 0.0F
    }
    val avg = parseUserAvgRate(user_movie_rates)
    val variance = user_movie_rates
      .map { case (_, rate) =>
        val diff = rate.toFloat - avg
        diff * diff
      }
      .sum / user_movie_rates.length.toFloat
    math.sqrt(variance.toDouble).toFloat
  }

  /**
   * Parse a join_sample row into ML1MSample with all feature groups.
   * Handles JSON parsing for user_profile, item_feature, and user_behavior fields.
   * Computes derived features: label, context (hour/weekday), genre-level aggregates, duration windows.
   *
   * @param row        ("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "item_feature", "user_behavior"))
   * @param movie_info Map[movie_id, (title, genres)]
   * @return (ML1MSample, successFlag)
   */
  def parseSample(row: Row, movie_info: collection.Map[Int, (String, Array[String])]): (ML1MSample, Boolean) = {
    val train_sample = new ML1MSample()
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
      train_sample.time_stamp = ts
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
        case e: Exception =>
          System.err.println(s"Warning: failed to parse publish year: ${e.getMessage}")
          0
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

    // parse user behavior
    json = row.getAs[String]("user_behavior")
    if (json != null && json != "{}") {
      val user_behavior = JSON.parseObject(json)
      // user_movie_rate
      val user_movie_rate_seq = user_behavior.getString("user_movie_rate")
      if (user_movie_rate_seq != null && user_movie_rate_seq.nonEmpty) {
        val userMovieRate30Days = ArrayBuffer.empty[(Int, Int)]
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
            val (_, genres) = movie_info.get(parts(0).toInt) match {
              case Some(tg) => tg
              case None => ("", Array.empty[String])
            }
            val dur = try {
              val ts = parseTimestampMillis(timestamp)
              (sample_timestamp - ts) / 1000.0 / 3600.0 / 24.0  // days between current rating and historical rating
            } catch {
              case e: Exception =>
                green_println("Parse user_movie_rate: " + e.toString + " " + item)
                ret = false
                -1.0
            }
            if (dur > 0) {  // include only strictly historical ratings
              train_sample.user_movie_rates.append((item_id, rate))
              train_sample.user_rate_cnt += 1
            }
            if (dur > 0 && dur <= 1) {
              train_sample.user_movie_rate_1days.append((item_id, rate))
            }
            if (dur > 0 && dur <= 3) {
              train_sample.user_movie_rate_3days.append((item_id, rate))
            }
            if (dur > 0 && dur <= 7) {
              train_sample.user_movie_rate_7days.append((item_id, rate))
              train_sample.user_rate_7day_cnt += 1
            }
            if (dur > 0 && dur <= 15) {
              train_sample.user_movie_rate_15days.append((item_id, rate))
              train_sample.user_rate_15day_cnt += 1
            }
            if (dur > 0 && dur <= 30) {
              userMovieRate30Days.append((item_id, rate))
              train_sample.user_rate_30day_cnt += 1
            }

            for (g <- genres) {
              val gen = g.trim.toLowerCase()
              if (dur > 0) {
                user_genres_rate_cnt_map(gen) += 1
                user_genres_rate_map(gen) += rate
              }
              if (dur > 0 && dur <= 1) {
                user_genres_rate_cnt_1day_map(gen) += 1
                user_genres_rate_1day_map(gen) += rate
              }
              if (dur > 0 && dur <= 3) {
                user_genres_rate_cnt_3day_map(gen) += 1
                user_genres_rate_3day_map(gen) += rate
              }
              if (dur > 0 && dur <= 7) {
                user_genres_rate_cnt_7day_map(gen) += 1
                user_genres_rate_7day_map(gen) += rate
              }
              if (dur > 0 && dur <= 15) {
                user_genres_rate_cnt_15day_map(gen) += 1
                user_genres_rate_15day_map(gen) += rate
              }
            }
          }
        }

        train_sample.user_avg_rate = parseUserAvgRate(train_sample.user_movie_rates)
        train_sample.user_rate_std = parseUserRateStd(train_sample.user_movie_rates)
        train_sample.user_avg_rate_7day = parseUserAvgRate(train_sample.user_movie_rate_7days)
        train_sample.user_rate_std_7day = parseUserRateStd(train_sample.user_movie_rate_7days)
        train_sample.user_avg_rate_15day = parseUserAvgRate(train_sample.user_movie_rate_15days)
        train_sample.user_rate_std_15day = parseUserRateStd(train_sample.user_movie_rate_15days)
        train_sample.user_avg_rate_30day = parseUserAvgRate(userMovieRate30Days)
        train_sample.user_rate_std_30day = parseUserRateStd(userMovieRate30Days)

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
