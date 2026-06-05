package encoder

import vectorizer.{CrossFeature, RawFeature, Target}
import vectorizer.FeatureEncoder
import sample.ML1MTrainSample
import utils.MurmurHash3

/**
 * ML1M 样本特征编码器
 *
 * 单值特征:
 *    整型 (Int/Long) -> 可不处理
 *    浮点型 (Float/Double) -> 分桶变为整型
 *    字符串 (String) -> hash变为整型
 *
 * 多值特征:
 *    遍历每个元素, 逐个加入. 若元素为String则先hash
 *
 * @author shard zhang
 * @date 2026/6/3 16:08
 * @note
 */
class FeatureEncoder4ML1M extends FeatureEncoder[ML1MTrainSample] {
  private type T = ML1MTrainSample

  // ============================== target ==============================
  /**
   * 多分类标签
   */
  private class TargetID extends Target[T] {
    override def parse(sample: T): Target[T] = {
      target = sample.target
      this
    }
  }

  /**
   * 二分类标签
   */
  private class Label extends Target[T] {
    override def parse(sample: T): Target[T] = {
      target = if (sample.rating >= 3) {
        1.0F
      } else {
        0.0F
      }
      this
    }
  }

  /**
   * 相关性回归标签
   */
  private class Rating extends Target[T] {
    override def parse(sample: T): Target[T] = {
      target = sample.rating
      this
    }
  }

  // ============================== item ==============================

  /**
   * movie_id
   */
  private class MovieID(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val movie_id = try {
        sample.item_id.toInt
      } catch {
        case _: Exception => 0
      }
      feature_list.append(movie_id)
      this
    }
  }

  /**
   * movie_title. 
   * 电影标题多值特征
   * 由于title几乎是唯一标识, 和 movieId 信息重复. 直接处理title信息冗余, 无效果增益
   */
  private class MovieTitle(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      try {
        val cleanTitle = sample.movie_title.replaceAll("\\s*\\(\\d+\\)\\s*$", "").trim
        val words = cleanTitle.split("\\s+")
        for (word <- words) {
          if (word.nonEmpty) {
            val p = new MurmurHash3.LongPair()
            MurmurHash3.murmurhash3_x64_128(word.getBytes, 0, word.length, SEED, p)
            feature_list.append(p.val1)
          }
        }
      } catch {
        case _: Exception =>
      }
      this
    }
  }

  /**
   * movie_geners
   * 电影类型标签
   */
  private class MovieGenres(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (gen <- sample.movie_genres) {
        if (gen != null && gen.nonEmpty) {
          val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
          MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
          feature_list.append(p.val1)
        }
      }
      this
    }
  }

  /**
   * movie_rate_count
   * 电影评论次数分桶
   */
  private class MovieRateCount(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val buck = sample.movie_rate_count match {
        case 0 => 0
        case x if x <= 2 => 1
        case x if x <= 5 => 2
        case x if x <= 10 => 3
        case x if x <= 20 => 4
        case x if x <= 50 => 5
        case x if x <= 100 => 6
        case x if x <= 200 => 7
        case x if x <= 500 => 8
        case x if x <= 1000 => 9
        case _ => 10
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 电影平均评分分桶特征
   */
  private class MovieAvgRate(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val buck = sample.movie_avg_rate match {
        case x if x <= 0.0 => 0
        case x if x <= 1.0 => 1
        case x if x <= 2.0 => 2
        case x if x <= 2.5 => 3
        case x if x <= 3.0 => 4
        case x if x <= 3.5 => 5
        case x if x <= 4.0 => 6
        case x if x <= 4.5 => 7
        case _ => 8
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 电影类型数量分桶特征
   * 一个电影属于多少种类型. 1类/2类/3类及以上, 类型越多受众越广
   */
  private class MovieGenreCnt(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val cnt = sample.movie_genres.size
      // 1/2/3+
      val buck = if (cnt >= 3) 3 else cnt
      feature_list.append(buck)
      this
    }
  }

  /**
   * 电影热度排名分桶
   * 解决冷启动: 爆款 / 热门 / 普通 / 长尾
   */
  private class MovieHotRank(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val rank = try {
        sample.movie_hot_rank
      } catch {
        case _: Exception => 99999
      }
      val buck = rank match {
        case x if x <= 100 => 3   // 爆款
        case x if x <= 500 => 2   // 热门
        case x if x <= 2000 => 1  // 中等
        case _ => 0               // 长尾冷门
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 电影上映年份 (从 title 正则提取)
   */
  private class MoviePublishYear(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val buck = sample.movie_publish_year match {
        case x if x == 0 => 0
        case x if x < 1970 => 1
        case x if x < 1980 => 2
        case x if x < 1990 => 3
        case x if x < 2000 => 4
        case x if x < 2010 => 5
        case _ => 6
      }
      feature_list.append(buck)
      this
    }
  }

  // ============================== user ==============================
  /**
   * user_id
   */
  private class UserID(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val user_id = try {
        sample.user_id.toInt
      } catch {
        case _: Exception => 0
      }
      feature_list.append(user_id)
      this
    }
  }

  /**
   * user_age
   * 用户年龄分桶特征
   */
  private class UserAge(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val age = try {
        sample.age.toInt
      } catch {
        case _: Exception => 0
      }
      feature_list.append(age)
      this
    }
  }

  /**
   * user_gender
   */
  private class UserGender(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hash = sample.gender match {
        case "M" => 1
        case "F" => 2
        case _ => 0
      }
      feature_list.append(hash)
      this
    }
  }

  /**
   * user_occupation
   */
  private class UserOccupation(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val occupation = try {
        sample.occupation.toInt
      } catch {
        case _: Exception => 0
      }
      feature_list.append(occupation)
      this
    }
  }

  /**
   * user_zipcode
   */
  private class UserZipCode(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hash = try {
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(sample.zip_code.getBytes(), 0, sample.zip_code.length, SEED, p)
        p.val1
      } catch {
        case _: Exception => 0L
      }
      feature_list.append(hash)
      this
    }
  }

  /**
   * 用户评分方差分桶
   * 挑剔用户(方差大) / 佛系用户(方差小)
   */
  private class UserRateStd(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val std = try {
        sample.user_rate_std
      } catch {
        case _: Exception => 0.0
      }
      // 分桶: 0=完全一致, 1=稳定, 2=一般, 3=挑剔
      val buck = std match {
        case x if x <= 0.0 => 0
        case x if x <= 1.0 => 1
        case x if x <= 2.0 => 2
        case _ => 3
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserRateStd7Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val std = try {
        sample.user_rate_std_7day
      } catch {
        case _: Exception => 0.0
      }
      // 分桶: 0=完全一致, 1=稳定, 2=一般, 3=挑剔
      val buck = std match {
        case x if x <= 0.0 => 0
        case x if x <= 1.0 => 1
        case x if x <= 2.0 => 2
        case _ => 3
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserRateStd15Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val std = try {
        sample.user_rate_std_15day
      } catch {
        case _: Exception => 0.0
      }
      // 分桶: 0=完全一致, 1=稳定, 2=一般, 3=挑剔
      val buck = std match {
        case x if x <= 0.0 => 0
        case x if x <= 1.0 => 1
        case x if x <= 2.0 => 2
        case _ => 3
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserRateStd30Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val std = try {
        sample.user_rate_std_30day
      } catch {
        case _: Exception => 0.0
      }
      // 分桶: 0=完全一致, 1=稳定, 2=一般, 3=挑剔
      val buck = std match {
        case x if x <= 0.0 => 0
        case x if x <= 1.0 => 1
        case x if x <= 2.0 => 2
        case _ => 3
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 用户活跃天数分桶
   * 区分: 新用户 / 老用户 / 超级用户
   */
  private class UserActiveDay(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val days = try {
        sample.user_active_day
      } catch {
        case _: Exception => 0
      }
      val buck = days match {
        case 0 => 0
        case x if x <= 7 => 1   // 1周内
        case x if x <= 30 => 2  // 1月内
        case _ => 3             // 老用户
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 用户总打分次数 (用户行为丰富度) 
   */
  private class UserMovieRateCnt(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val cnt = try {
        sample.user_rate_cnt
      } catch {
        case _: Exception => 0
      }
      // 分桶: 用户打分数量区间
      val buck = cnt match {
        case x if x <= 10 => 1
        case x if x <= 30 => 2
        case x if x <= 50 => 3
        case x if x <= 100 => 4
        case _ => 5
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserMovieRateCnt7Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val cnt = try {
        sample.user_rate_7day_cnt
      } catch {
        case _: Exception => 0
      }
      // 分桶: 用户打分数量区间
      val buck = cnt match {
        case x if x <= 10 => 1
        case x if x <= 30 => 2
        case x if x <= 50 => 3
        case x if x <= 100 => 4
        case _ => 5
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserMovieRateCnt15Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val cnt = try {
        sample.user_rate_15day_cnt
      } catch {
        case _: Exception => 0
      }
      // 分桶: 用户打分数量区间
      val buck = cnt match {
        case x if x <= 10 => 1
        case x if x <= 30 => 2
        case x if x <= 50 => 3
        case x if x <= 100 => 4
        case _ => 5
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserMovieRateCnt30Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val cnt = try {
        sample.user_rate_30day_cnt
      } catch {
        case _: Exception => 0
      }
      // 分桶: 用户打分数量区间
      val buck = cnt match {
        case x if x <= 10 => 1
        case x if x <= 30 => 2
        case x if x <= 50 => 3
        case x if x <= 100 => 4
        case _ => 5
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 用户历史平均分: 区分低分/中庸/高分偏好
   */
  private class UserAvgRate(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val avg = try {
        sample.user_avg_rate
      } catch {
        case _: Exception => 3.0
      }
      // 低分用户(＜3)、中庸3、高分偏好(≥4)
      val buck = avg match {
        case x if x < 3.0 => 1 // 低分用户
        case x if x < 4.0 => 2 // 中庸
        case _ => 3 // 高分偏好
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserAvgRate7Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val avg = try {
        sample.user_avg_rate_7day
      } catch {
        case _: Exception => 3.0
      }
      // 低分用户(＜3)、中庸3、高分偏好(≥4)
      val buck = avg match {
        case x if x < 3.0 => 1 // 低分用户
        case x if x < 4.0 => 2 // 中庸
        case _ => 3 // 高分偏好
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserAvgRate15Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val avg = try {
        sample.user_avg_rate_15day
      } catch {
        case _: Exception => 3.0
      }
      // 低分用户(＜3)、中庸3、高分偏好(≥4)
      val buck = avg match {
        case x if x < 3.0 => 1 // 低分用户
        case x if x < 4.0 => 2 // 中庸
        case _ => 3 // 高分偏好
      }
      feature_list.append(buck)
      this
    }
  }

  private class UserAvgRate30Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val avg = try {
        sample.user_avg_rate_30day
      } catch {
        case _: Exception => 3.0
      }
      // 低分用户(＜3)、中庸3、高分偏好(≥4)
      val buck = avg match {
        case x if x < 3.0 => 1 // 低分用户
        case x if x < 4.0 => 2 // 中庸
        case _ => 3 // 高分偏好
      }
      feature_list.append(buck)
      this
    }
  }

  /**
   * 用户历史最爱 3 个电影类型 -> Multi-hot 特征
   */
  private class UserTop3Genres(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      try {
        val topGenres = sample.user_top3_genres
        for ((g, cnt) <- topGenres) {
          if (g != null && g.nonEmpty) {
            val p = new MurmurHash3.LongPair()
            MurmurHash3.murmurhash3_x64_128(g.getBytes, 0, g.length, SEED, p)
            feature_list.append(p.val1)
          }
        }
      } catch {
        case _: Exception =>
      }
      this
    }
  }

  /**
   * 用户历史是否看过 [当前电影] 所属类型 (0=未看过/1=看过)
   * 长期兴趣信号
   */
  private class UserWatchSameGenre(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hit = try {
        val genres = sample.movie_genres.toSet
        val user_genres_rate = sample.user_genres_rates.map(_._1).toSet
        val hasOverlap = if (genres.isEmpty || user_genres_rate.isEmpty) {
          0
        } else if (genres.intersect(user_genres_rate).nonEmpty) {
          1
        } else {
          0
        }
        hasOverlap
      } catch {
        case _: Exception => 0
      }
      feature_list.append(hit)
      this
    }
  }

  /**
   * 用户近 1 天是否看过 [当前电影] 所属类型 (0=未看过/1=看过)
   * 短期兴趣信号
   */
  private class UserWatchSameGenre1Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_1days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 1 else 0
        flag
      } catch {
        case _: Exception => 0
      }
      feature_list.append(hit)
      this
    }
  }

  /**
   * 用户近 3 天是否看过 [当前电影] 所属类型 (0=未看过/1=看过)
   * 短期兴趣信号
   */
  private class UserWatchSameGenre3Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_3days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 1 else 0
        flag
      } catch {
        case _: Exception => 0
      }
      feature_list.append(hit)
      this
    }
  }

  /**
   * 用户近 7 天是否看过 [当前电影] 所属类型 (0=未看过/1=看过)
   * 短期兴趣信号
   */
  private class UserWatchSameGenre7Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_7days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 1 else 0
        flag
      } catch {
        case _: Exception => 0
      }
      feature_list.append(hit)
      this
    }
  }

  /**
   * 用户近 15 天是否看过 [当前电影] 所属类型 (0=未看过/1=看过)
   * 短期兴趣信号
   */
  private class UserWatchSameGenre15Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val hit = try {
        val currentGenres = sample.movie_genres.toSet
        val recentGenres = sample.user_genres_rate_15days.map(_._1).toSet
        val flag = if (currentGenres.intersect(recentGenres).nonEmpty) 1 else 0
        flag
      } catch {
        case _: Exception => 0
      }
      feature_list.append(hit)
      this
    }
  }

  /**
   * 用户对 [当前电影所有类型] 的历史平均分桶
   */
  private class UserSameGenreAvgRate(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val buk = try {
        val user_genre_avg_rate: Map[String, Float] = sample.user_genres_rates.toMap
        val genres: Seq[String] = sample.movie_genres
        val rates: Seq[Float] = genres.flatMap { g =>
          user_genre_avg_rate.get(g)
        }
        val finalRate = if (rates.isEmpty) {
          3.0 // 无历史, 中性默认分
        } else {
          rates.sum / rates.size // 多个类型取平均
        }
        finalRate match {
          case x if x <= 1.0 => 1
          case x if x <= 2.0 => 2
          case x if x <= 3.0 => 3
          case x if x <= 4.0 => 4
          case _ => 5
        }
      } catch {
        case _: Exception => 3
      }
      feature_list.append(buk)
      this
    }
  }

  // ============================== context ==============================
  /**
   * context_time_hour
   * 小时分桶
   */
  private class ContextTimeHour(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      feature_list.append(sample.time_hour + 1)
      this
    }
  }

  /**
   * context_time_area
   * 时区分桶
   */
  private class ContextTimeArea(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      feature_list.append(sample.time_area + 1)
      this
    }
  }

  /**
   * context_time_week
   * 星期分桶
   */
  private class ContextTimeWeek(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      feature_list.append(sample.week_day)
      this
    }
  }

  /**
   * 是否周末: 1=周末(6/7), 0=工作日
   */
  private class IsWeekend(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      val w = sample.week_day
      val flag = if (w == 6 || w == 7) 2 else 1
      feature_list.append(flag)
      this
    }
  }

  // ============================== user behavior ==============================
  /**
   * user_movie_rate
   * 用户电影评分序列(历史全部)
   */
  private class UserMovieRate(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_movie_rates.size)) {
        feature_list.append(sample.user_movie_rates(i)._1)
      }
      this
    }
  }

  private class UserMovieRate1Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_1days.size)) {
        feature_list.append(sample.user_movie_rate_1days(i)._1)
      }
      this
    }
  }

  private class UserMovieRate3Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_3days.size)) {
        feature_list.append(sample.user_movie_rate_3days(i)._1)
      }
      this
    }
  }

  private class UserMovieRate7Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_7days.size)) {
        feature_list.append(sample.user_movie_rate_7days(i)._1)
      }
      this
    }
  }

  private class UserMovieRate15Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_movie_rate_15days.size)) {
        feature_list.append(sample.user_movie_rate_15days(i)._1)
      }
      this
    }
  }

  /**
   * user_genres_rates
   * 用户电影类型评分序列(历史全部)
   */
  private class UserGenresRate(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rates.size)) {
        val gen = sample.user_genres_rates(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        feature_list.append(p.val1)
      }
      this
    }
  }

  private class UserGenresRate1Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_1days.size)) {
        val gen = sample.user_genres_rate_1days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        feature_list.append(p.val1)
      }
      this
    }
  }

  private class UserGenresRate3Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_3days.size)) {
        val gen = sample.user_genres_rate_3days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        feature_list.append(p.val1)
      }
      this
    }
  }

  private class UserGenresRate7Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_7days.size)) {
        val gen = sample.user_genres_rate_7days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        feature_list.append(p.val1)
      }
      this
    }
  }

  private class UserGenresRate15Day(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_15days.size)) {
        val gen = sample.user_genres_rate_15days(i)._1
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        feature_list.append(p.val1)
      }
      this
    }
  }

  /**
   * user_genres_rate_cnts
   * 用户电影类型评分次数序列(历史全部)
   */
  private class UserGenresRateCnts(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnts.size)) {
        val gen = sample.user_genres_rate_cnts(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnts(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        for (k <- 0 until total_cnt) {
          feature_list.append(p.val1)
        }
      }
      this
    }
  }

  private class UserGenresRateCnt1Days(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_1days.size)) {
        val gen = sample.user_genres_rate_cnt_1days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_1days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        for (k <- 0 until total_cnt) {
          feature_list.append(p.val1)
        }
      }
      this
    }
  }

  private class UserGenresRateCnt3Days(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_3days.size)) {
        val gen = sample.user_genres_rate_cnt_3days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_3days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        for (k <- 0 until total_cnt) {
          feature_list.append(p.val1)
        }
      }
      this
    }
  }

  private class UserGenresRateCnt7Days(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_7days.size)) {
        val gen = sample.user_genres_rate_cnt_7days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_7days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        for (k <- 0 until total_cnt) {
          feature_list.append(p.val1)
        }
      }
      this
    }
  }

  private class UserGenresRateCnt15Days(f_i: Int, f_n: String) extends RawFeature[T](f_i, f_n) {
    override def parse(sample: T): RawFeature[T] = {
      for (i <- 0 until Math.min(200, sample.user_genres_rate_cnt_15days.size)) {
        val gen = sample.user_genres_rate_cnt_15days(i)._1.trim.toLowerCase()
        val total_cnt = sample.user_genres_rate_cnt_15days(i)._2
        val p: MurmurHash3.LongPair = new MurmurHash3.LongPair()
        MurmurHash3.murmurhash3_x64_128(gen.getBytes(), 0, gen.length, SEED, p)
        for (k <- 0 until total_cnt) {
          feature_list.append(p.val1)
        }
      }
      this
    }
  }

  override def setup(): FeatureEncoder[T] = {
    raw_features.clear()
    cross_features.clear()

    // ============================== target ==============================
    target = new Rating()

    // ============================== user ==============================
    val user_id = new UserID(1, "user_id")
    val user_age = new UserAge(2, "user_age")
    val user_gender = new UserGender(3, "user_gender")
    val user_occupation = new UserOccupation(4, "user_occupation")
    val user_zip_code = new UserZipCode(5, "user_zip_code")
    raw_features.append(user_id)
    raw_features.append(user_age)
    raw_features.append(user_gender)
    raw_features.append(user_occupation)
    raw_features.append(user_zip_code)

    val user_rate_std = new UserRateStd(6, "user_rate_std")
    val user_rate_std_7day = new UserRateStd7Day(7, "user_rate_std_7day")
    val user_rate_std_15day = new UserRateStd15Day(8, "user_rate_std_15day")
    val user_rate_std_30day = new UserRateStd30Day(9, "user_rate_std_30day")
    raw_features.append(user_rate_std)
    raw_features.append(user_rate_std_7day)
    raw_features.append(user_rate_std_15day)
    raw_features.append(user_rate_std_30day)

    val user_movie_rate_cnt = new UserMovieRateCnt(10, "user_movie_rate_cnt")
    val user_movie_rate_cnt_7day = new UserMovieRateCnt7Day(11, "user_movie_rate_cnt_7day")
    val user_movie_rate_cnt_15day = new UserMovieRateCnt15Day(12, "user_movie_rate_cnt_15day")
    val user_movie_rate_cnt_30day = new UserMovieRateCnt30Day(13, "user_movie_rate_cnt_30day")
    raw_features.append(user_movie_rate_cnt)
    raw_features.append(user_movie_rate_cnt_7day)
    raw_features.append(user_movie_rate_cnt_15day)
    raw_features.append(user_movie_rate_cnt_30day)

    val user_avg_rate = new UserAvgRate(14, "user_avg_rate")
    val user_avg_rate_7day = new UserAvgRate7Day(15, "user_avg_rate_7day")
    val user_avg_rate_15day = new UserAvgRate15Day(16, "user_avg_rate_15day")
    val user_avg_rate_30day = new UserAvgRate30Day(17, "user_avg_rate_30day")
    raw_features.append(user_avg_rate)
    raw_features.append(user_avg_rate_7day)
    raw_features.append(user_avg_rate_15day)
    raw_features.append(user_avg_rate_30day)

    // user lifecycle
    val user_active_days = new UserActiveDay(20, "user_active_days")
    raw_features.append(user_active_days)


    // ============================== item ==============================
    val movie_id = new MovieID(101, "movie_id")
    val movie_title = new MovieTitle(102, "movie_title")
    val movie_genres = new MovieGenres(103, "movie_genres")
    raw_features.append(movie_id)
    raw_features.append(movie_title)
    raw_features.append(movie_genres)

    val movie_rate_count = new MovieRateCount(104, "movie_rate_count")
    val movie_avg_rate = new MovieAvgRate(105, "movie_avg_rate")
    val movie_genre_cnt = new MovieGenreCnt(106, "movie_genre_cnt")
    val movie_hot_rank = new MovieHotRank(107, "item_hot_rank")
    val movie_publish_year = new MoviePublishYear(108, "movie_publish_year")
    raw_features.append(movie_rate_count)
    raw_features.append(movie_avg_rate)
    raw_features.append(movie_genre_cnt)
    raw_features.append(movie_hot_rank)
    raw_features.append(movie_publish_year)


    // ============================== context ==============================
    val context_time_hour = new ContextTimeHour(201, "context_time_hour")
    val context_time_area = new ContextTimeArea(202, "context_time_area")
    val context_time_week = new ContextTimeWeek(203, "context_time_week")
    val context_is_weekend = new IsWeekend(204, "context_is_weekend")
    raw_features.append(context_time_hour)
    raw_features.append(context_time_area)
    raw_features.append(context_time_week)
    raw_features.append(context_is_weekend)


    // ============================== user behavior ==============================
    val user_movie_rate = new UserMovieRate(301, "user_movie_rate")
    val user_movie_rate_1day = new UserMovieRate1Day(302, "user_movie_rate_1day")
    val user_movie_rate_3day = new UserMovieRate3Day(303, "user_movie_rate_3day")
    val user_movie_rate_7day = new UserMovieRate7Day(304, "user_movie_rate_7day")
    val user_movie_rate_15day = new UserMovieRate15Day(305, "user_movie_rate_15day")
    raw_features.append(user_movie_rate)
    raw_features.append(user_movie_rate_1day)
    raw_features.append(user_movie_rate_3day)
    raw_features.append(user_movie_rate_7day)
    raw_features.append(user_movie_rate_15day)

    val user_genres_rate = new UserGenresRate(306, "user_genres_rate")
    val user_genres_rate_1day = new UserGenresRate1Day(307, "user_genres_rate_1day")
    val user_genres_rate_3day = new UserGenresRate3Day(308, "user_genres_rate_3day")
    val user_genres_rate_7day = new UserGenresRate7Day(309, "user_genres_rate_7day")
    val user_genres_rate_15day = new UserGenresRate15Day(310, "user_genres_rate_15day")
    raw_features.append(user_genres_rate)
    raw_features.append(user_genres_rate_1day)
    raw_features.append(user_genres_rate_3day)
    raw_features.append(user_genres_rate_7day)
    raw_features.append(user_genres_rate_15day)

    val user_genres_rate_cnts = new UserGenresRateCnts(312, "user_genres_rate_cnts")
    val user_genres_rate_cnt_1days = new UserGenresRateCnt1Days(313, "user_genres_rate_cnt_1days")
    val user_genres_rate_cnt_3days = new UserGenresRateCnt3Days(314, "user_genres_rate_cnt_3days")
    val user_genres_rate_cnt_7days = new UserGenresRateCnt7Days(315, "user_genres_rate_cnt_7days")
    val user_genres_rate_cnt_15days = new UserGenresRateCnt15Days(316, "user_genres_rate_cnt_15days")
    raw_features.append(user_genres_rate_cnts)
    raw_features.append(user_genres_rate_cnt_1days)
    raw_features.append(user_genres_rate_cnt_3days)
    raw_features.append(user_genres_rate_cnt_7days)
    raw_features.append(user_genres_rate_cnt_15days)

    val user_top3_genres = new UserTop3Genres(317, "user_top3_genres")
    raw_features.append(user_top3_genres)

    val user_watch_same_genre = new UserWatchSameGenre(351, "user_watch_same_genre")
    val user_watch_same_genre_1day = new UserWatchSameGenre1Day(352, "user_watch_same_genre_1day")
    val user_watch_same_genre_3day = new UserWatchSameGenre3Day(353, "user_watch_same_genre_3day")
    val user_watch_same_genre_7day = new UserWatchSameGenre7Day(354, "user_watch_same_genre_7day")
    val user_watch_same_genre_15day = new UserWatchSameGenre15Day(355, "user_watch_same_genre_15day")
    val user_same_genre_avg_rate = new UserSameGenreAvgRate(356, "user_same_genre_avg_rate")
    raw_features.append(user_watch_same_genre)
    raw_features.append(user_watch_same_genre_1day)
    raw_features.append(user_watch_same_genre_3day)
    raw_features.append(user_watch_same_genre_7day)
    raw_features.append(user_watch_same_genre_15day)
    raw_features.append(user_same_genre_avg_rate)

    // ============================== cross feature: Second-order ==============================
    // {{类型 x 时间窗口兴趣}}
    // 电影类型 x 用户全历史偏好类型 (长期兴趣匹配，核心强特征)
    val movie_genres_xx_user_genres_rate = new CrossFeature(401, "movie_genres_xx_user_genres_rate", movie_genres, user_genres_rate)
    // 电影类型 x 用户近1天偏好类型 (超短期实时兴趣，时效性强)
    val movie_genres_xx_user_genres_rate_1day = new CrossFeature(402, "movie_genres_xx_user_genres_rate_1day", movie_genres, user_genres_rate_1day)
    // 电影类型 x 用户近3天偏好类型 (短期实时兴趣)
    val movie_genres_xx_user_genres_rate_3day = new CrossFeature(403, "movie_genres_xx_user_genres_rate_3day", movie_genres, user_genres_rate_3day)
    // 电影类型 x 用户近7天偏好类型 (中期兴趣偏好)
    val movie_genres_xx_user_genres_rate_7day = new CrossFeature(404, "movie_genres_xx_user_genres_rate_7day", movie_genres, user_genres_rate_7day)
    // 电影类型 x 用户近15天偏好类型 (中长期兴趣偏好)
    val movie_genres_xx_user_genres_rate_15day = new CrossFeature(405, "movie_genres_xx_user_genres_rate_15day", movie_genres, user_genres_rate_15day)
    cross_features.append(movie_genres_xx_user_genres_rate)
    cross_features.append(movie_genres_xx_user_genres_rate_1day)
    cross_features.append(movie_genres_xx_user_genres_rate_3day)
    cross_features.append(movie_genres_xx_user_genres_rate_7day)
    cross_features.append(movie_genres_xx_user_genres_rate_15day)

    // {{物品基础 x 用户基础}}
    // 电影上映年份 x 用户年龄 (年代偏好 + 年龄圈层匹配)
    val movie_publish_year_xx_user_age = new CrossFeature(406, "movie_publish_year_xx_user_age", movie_publish_year, user_age)
    cross_features.append(movie_publish_year_xx_user_age)

    // {{物品统计 x 用户统计}}
    // 电影热度排名 x 用户活跃天数 (冷启动核心: 新用户爱热门，老用户爱小众)
    val movie_hot_rank_xx_user_active_days = new CrossFeature(407, "movie_hot_rank_xx_user_active_days", movie_hot_rank, user_active_days)
    // 电影均分 x 用户历史平均分 (用户品味 x 影片质量匹配)
    val movie_avg_rate_xx_user_avg_rate = new CrossFeature(408, "movie_avg_rate_xx_user_avg_rate", movie_avg_rate, user_avg_rate)
    // 电影类型数量 x 用户总打分次数 (行为深度 x 影片受众广度)
    val movie_genre_cnt_xx_user_total_rate_cnt = new CrossFeature(409, "movie_genre_cnt_xx_user_total_rate_cnt", movie_genre_cnt, user_movie_rate_cnt)
    cross_features.append(movie_hot_rank_xx_user_active_days)
    cross_features.append(movie_avg_rate_xx_user_avg_rate)
    cross_features.append(movie_genre_cnt_xx_user_total_rate_cnt)

    // {{物品统计 x 用户偏好}}
    // 电影评分人数 x 用户打分标准差 (影片流行度 x 用户打分偏好稳定性)
    val movie_rate_count_xx_user_rate_std = new CrossFeature(410, "movie_rate_count_xx_user_rate_std", movie_rate_count, user_rate_std)
    // 电影热度 x 用户历史平均分 (热门度 x 用户评分偏好)
    val movie_hot_rank_xx_user_avg_rate = new CrossFeature(411, "movie_hot_rank_xx_user_avg_rate", movie_hot_rank, user_avg_rate)
    // 电影上映年份 x 用户历史平均分 (年代偏好 x 用户评分风格)
    val movie_publish_year_xx_user_avg_rate = new CrossFeature(412, "movie_publish_year_xx_user_avg_rate", movie_publish_year, user_avg_rate)
    // 电影类型数量 x 用户历史平均分 (类型复杂度 x 用户评分偏好)
    val movie_genre_cnt_xx_user_avg_rate = new CrossFeature(413, "movie_genre_cnt_xx_user_avg_rate", movie_genre_cnt, user_avg_rate)
    // 电影热度 x 用户类型偏好均分 (热度 x 细分类型喜爱度)
    val movie_hot_rank_xx_user_genre_avg_rate = new CrossFeature(414, "movie_hot_rank_xx_user_genre_avg_rate", movie_hot_rank, user_same_genre_avg_rate)
    cross_features.append(movie_rate_count_xx_user_rate_std)
    cross_features.append(movie_hot_rank_xx_user_avg_rate)
    cross_features.append(movie_publish_year_xx_user_avg_rate)
    cross_features.append(movie_genre_cnt_xx_user_avg_rate)
    cross_features.append(movie_hot_rank_xx_user_genre_avg_rate)

    // {{类型 x 用户属性}}
    // 用户性别 x 电影类型 (男女偏好差异巨大)
    val movie_genres_xx_user_gender = new CrossFeature(417, "movie_genres_xx_user_gender", movie_genres, user_gender)
    // 用户职业 x 电影类型 (职业圈层偏好明显)
    val movie_genres_xx_user_occupation = new CrossFeature(418, "movie_genres_xx_user_occupation", movie_genres, user_occupation)
    // 电影类型 x 用户年龄 (不同年龄偏好类型差异大)
    val movie_genres_xx_user_age = new CrossFeature(419, "movie_genres_xx_user_age", movie_genres, user_age)
    cross_features.append(movie_genres_xx_user_gender)
    cross_features.append(movie_genres_xx_user_occupation)
    cross_features.append(movie_genres_xx_user_age)

    // {{物品 x 用户高阶统计}}
    // 电影均分 x 用户打分标准差 (影片质量 x 用户挑剔程度)
    val movie_avg_rate_xx_user_rate_std = new CrossFeature(420, "movie_avg_rate_xx_user_rate_std", movie_avg_rate, user_rate_std)
    // 电影热度 x 用户打分标准差 (强特征: 热度 x 用户挑剔程度)
    val movie_hot_rank_xx_user_rate_std = new CrossFeature(421, "movie_hot_rank_xx_user_rate_std", movie_hot_rank, user_rate_std)
    // 电影上映年份 x 用户活跃度 (年代偏好 x 用户活跃程度)
    val movie_publish_year_xx_user_active_days = new CrossFeature(422, "movie_publish_year_xx_user_active_days", movie_publish_year, user_active_days)
    // 电影类型数 x 用户活跃度 (类型广度 x 用户行为深度)
    val movie_genre_cnt_xx_user_active_days = new CrossFeature(423, "movie_genre_cnt_xx_user_active_days", movie_genre_cnt, user_active_days)
    // 电影热度 x 用户总打分次数 (热度 x 用户行为丰富度)
    val movie_hot_rank_xx_user_total_rate_cnt = new CrossFeature(424, "movie_hot_rank_xx_user_total_rate_cnt", movie_hot_rank, user_movie_rate_cnt)
    cross_features.append(movie_avg_rate_xx_user_rate_std)
    cross_features.append(movie_hot_rank_xx_user_rate_std)
    cross_features.append(movie_publish_year_xx_user_active_days)
    cross_features.append(movie_genre_cnt_xx_user_active_days)
    cross_features.append(movie_hot_rank_xx_user_total_rate_cnt)

    // {{节假日}}
    // 电影类型 x 是否周末 (周末/工作日观影类型差异极大)
    val movie_genres_xx_is_weekend = new CrossFeature(450, "movie_genres_xx_is_weekend", movie_genres, context_is_weekend)
    // 电影热度 x 是否周末 (周末更爱看热门, 工作日更爱看小众)
    val movie_hot_rank_xx_is_weekend = new CrossFeature(451, "movie_hot_rank_xx_is_weekend", movie_hot_rank, context_is_weekend)
    // 用户年龄 x 是否周末 (不同年龄周末观影偏好分层)
    val user_age_xx_is_weekend = new CrossFeature(452, "user_age_xx_is_weekend", user_age, context_is_weekend)
    // 用户性别 x 时段 (男女在不同时段观影偏好差异)
    val user_gender_xx_context_time_hour = new CrossFeature(453, "user_gender_xx_context_time_hour", user_gender, context_time_hour)
    cross_features.append(movie_genres_xx_is_weekend)
    cross_features.append(movie_hot_rank_xx_is_weekend)
    cross_features.append(user_age_xx_is_weekend)
    cross_features.append(user_gender_xx_context_time_hour)

    // ============================== cross feature: Third-order ==============================
    // 年龄 x 性别 x 影片类型 -> 人群圈层最核心强特征
    val movie_genres_xx_user_age_xx_user_gender = new CrossFeature(460, "movie_genres_xx_user_age_xx_user_gender", movie_genres, user_age, user_gender)
    cross_features.append(movie_genres_xx_user_age_xx_user_gender)

    // 性别 x 职业 x 影片类型 -> 人群细分强特征
    val movie_genres_xx_user_gender_xx_user_occupation = new CrossFeature(461, "movie_genres_xx_user_gender_xx_user_occupation", movie_genres, user_gender, user_occupation)
    cross_features.append(movie_genres_xx_user_gender_xx_user_occupation)

    // 年龄 x 职业 x 电影年份 -> 年代偏好分层
    val movie_publish_year_xx_user_age_xx_user_occupation = new CrossFeature(462, "movie_publish_year_xx_user_age_xx_user_occupation", movie_publish_year, user_age, user_occupation)
    cross_features.append(movie_publish_year_xx_user_age_xx_user_occupation)

    // 影片均分 x 热度 x 用户均分 -> 品味匹配核心特征
    val movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate = new CrossFeature(463, "movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate", movie_avg_rate, movie_hot_rank, user_avg_rate)
    cross_features.append(movie_avg_rate_xx_movie_hot_rank_xx_user_avg_rate)

    // 影片热度 x 用户活跃度 x 用户均分 -> 冷启动核心
    val movie_hot_rank_xx_user_active_days_xx_user_avg_rate = new CrossFeature(464, "movie_hot_rank_xx_user_active_days_xx_user_avg_rate", movie_hot_rank, user_active_days, user_avg_rate)
    cross_features.append(movie_hot_rank_xx_user_active_days_xx_user_avg_rate)

    // 影片类型数 x 用户总打分次数 x 用户均分 -> 行为深度+品味
    val movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate = new CrossFeature(465, "movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate", movie_genre_cnt, user_movie_rate_cnt, user_avg_rate)
    cross_features.append(movie_genre_cnt_xx_user_total_rate_cnt_xx_user_avg_rate)

    // 影片类型数 x 热度 x 用户类型均分 -> 类型偏好+热度匹配
    val movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate = new CrossFeature(466, "movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate", movie_genre_cnt, movie_hot_rank, user_same_genre_avg_rate)
    cross_features.append(movie_genre_cnt_xx_movie_hot_rank_xx_user_genre_avg_rate)

    // 影片年份 x 均分 x 用户均分 -> 年代+质量+用户品味
    val movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate = new CrossFeature(467, "movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate", movie_publish_year, movie_avg_rate, user_avg_rate)
    cross_features.append(movie_publish_year_xx_movie_avg_rate_xx_user_avg_rate)

    return this
  }
}
