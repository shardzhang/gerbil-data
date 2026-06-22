package featurizer.ml1m

import org.scalatest.{Matchers, WordSpec}
import featurizer.core.FieldType

import scala.collection.mutable.ArrayBuffer

class ML1MUserFeaturesTest extends WordSpec with Matchers {

  private def baseSample(): ML1MSample = {
    val s = new ML1MSample()
    s.user_id = "1"
    s.gender = "M"
    s.age = "25"
    s.occupation = "12"
    s.zip_code = "10001"
    s.user_rate_std = 1.5F
    s.user_rate_std_7day = 0.5F
    s.user_rate_std_15day = 0.0F
    s.user_rate_std_30day = 2.0F
    s.user_rate_cnt = 50
    s.user_rate_7day_cnt = 10
    s.user_rate_15day_cnt = 20
    s.user_rate_30day_cnt = 30
    s.user_avg_rate = 3.5F
    s.user_avg_rate_7day = 3.0F
    s.user_avg_rate_15day = 4.0F
    s.user_avg_rate_30day = 2.5F
    s.movie_genres = ArrayBuffer("animation", "comedy")
    s.user_genres_rates = ArrayBuffer(("animation", 3.5F), ("comedy", 4.0F))
    s.user_genres_rate_1days = ArrayBuffer(("action", 4.0F))
    s.user_genres_rate_3days = ArrayBuffer.empty
    s.user_genres_rate_7days = ArrayBuffer(("drama", 3.0F))
    s.user_genres_rate_15days = ArrayBuffer(("comedy", 3.5F))
    s.user_active_day = 30
    s.user_top3_genres = ArrayBuffer(("animation", 5), ("comedy", 3))
    s
  }

  "UserID" should {
    "parse user_id to int" in {
      val f = new UserID(1, "user_id")
      f.parse(baseSample())
      assert(f.raw_list.head === "1")
      assert(f.feature_list.head === 1)
    }
    "handle non-numeric with 0" in {
      val s = baseSample(); s.user_id = "abc"
      val f = new UserID(1, "user_id")
      f.parse(s)
      assert(f.feature_list.head === 0)
    }
  }

  "UserAge" should {
    "parse age to int" in {
      val f = new UserAge(2, "user_age")
      f.parse(baseSample())
      assert(f.raw_list.head === "25")
      assert(f.feature_list.head === 25)
    }
    "handle invalid with 0" in {
      val s = baseSample(); s.age = "invalid"
      val f = new UserAge(2, "user_age")
      f.parse(s)
      assert(f.feature_list.head === 0)
    }
  }

  "UserGender" should {
    "encode M as 1" in {
      val f = new UserGender(3, "user_gender")
      f.parse(baseSample())
      assert(f.feature_list.head === 1)
    }
    "encode F as 2" in {
      val s = baseSample(); s.gender = "F"
      val f = new UserGender(3, "user_gender")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }
    "encode other as 0" in {
      val s = baseSample(); s.gender = ""
      val f = new UserGender(3, "user_gender")
      f.parse(s)
      assert(f.feature_list.head === 0)
    }
  }

  "UserOccupation" should {
    "parse to int" in {
      val f = new UserOccupation(4, "user_occupation")
      f.parse(baseSample())
      assert(f.feature_list.head === 12)
    }
    "handle invalid with 0" in {
      val s = baseSample(); s.occupation = "abc"
      val f = new UserOccupation(4, "user_occupation")
      f.parse(s)
      assert(f.feature_list.head === 0)
    }
  }

  "UserZipCode" should {
    "hash zip_code" in {
      val f = new UserZipCode(5, "user_zip_code")
      f.parse(baseSample())
      assert(f.raw_list.head === "10001")
      assert(f.feature_list.head > 0)
    }
  }

  "UserRateStd" should {
    "bucket 1.5 to 3" in {
      val f = new UserRateStd(6, "user_rate_std")
      f.parse(baseSample())
      assert(f.feature_list.head === 3)
    }
    "bucket 0.0 to 1" in {
      val s = baseSample(); s.user_rate_std = 0.0F
      val f = new UserRateStd(6, "user_rate_std")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
    "bucket 0.8 to 2" in {
      val s = baseSample(); s.user_rate_std = 0.8F
      val f = new UserRateStd(6, "user_rate_std")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }
    "bucket >2 to 4" in {
      val s = baseSample(); s.user_rate_std = 3.0F
      val f = new UserRateStd(6, "user_rate_std")
      f.parse(s)
      assert(f.feature_list.head === 4)
    }
  }

  "UserRateStdContinue" should {
    "use raw value" in {
      val f = new UserRateStdContinue(18, "user_rate_std_continue")
      f.parse(baseSample())
      assert(f.value_list.head === 1.5F)
      assert(f.feature_list.head === 1L)
    }
  }

  "UserRateStd7Day" should {
    "bucket 0.5 to 2" in {
      val f = new UserRateStd7Day(7, "user_rate_std_7day")
      f.parse(baseSample())
      assert(f.feature_list.head === 2)
    }
  }

  "UserMovieRateCnt" should {
    "bucket 50 to 4" in {
      val f = new UserMovieRateCnt(10, "user_movie_rate_cnt")
      f.parse(baseSample())
      assert(f.feature_list.head === 4)
    }
    "bucket 0 to 1" in {
      val s = baseSample(); s.user_rate_cnt = 0
      val f = new UserMovieRateCnt(10, "user_movie_rate_cnt")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
    "bucket 5 to 2" in {
      val s = baseSample(); s.user_rate_cnt = 5
      val f = new UserMovieRateCnt(10, "user_movie_rate_cnt")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }
  }

  "UserAvgRate" should {
    "bucket 3.5 to 3" in {
      val f = new UserAvgRate(14, "user_avg_rate")
      f.parse(baseSample())
      assert(f.feature_list.head === 3)
    }
    "bucket 0.0 to 1" in {
      val s = baseSample(); s.user_avg_rate = 0.0F
      val f = new UserAvgRate(14, "user_avg_rate")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
    "bucket 2.0 to 2" in {
      val s = baseSample(); s.user_avg_rate = 2.0F
      val f = new UserAvgRate(14, "user_avg_rate")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }
    "bucket >=4 to 4" in {
      val s = baseSample(); s.user_avg_rate = 4.5F
      val f = new UserAvgRate(14, "user_avg_rate")
      f.parse(s)
      assert(f.feature_list.head === 4)
    }
  }

  "UserAvgRateContinue" should {
    "use raw value" in {
      val f = new UserAvgRateContinue(23, "user_avg_rate_continue")
      f.parse(baseSample())
      assert(f.value_list.head === 3.5F)
    }
  }

  "UserActiveDay" should {
    "bucket 30 to 3" in {
      val f = new UserActiveDay(20, "user_active_day")
      f.parse(baseSample())
      assert(f.feature_list.head === 3)
    }
    "bucket 0 to 1" in {
      val s = baseSample(); s.user_active_day = 0
      val f = new UserActiveDay(20, "user_active_day")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
    "bucket 5 to 2" in {
      val s = baseSample(); s.user_active_day = 5
      val f = new UserActiveDay(20, "user_active_day")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }
    "bucket >30 to 4" in {
      val s = baseSample(); s.user_active_day = 100
      val f = new UserActiveDay(20, "user_active_day")
      f.parse(s)
      assert(f.feature_list.head === 4)
    }
  }

  "UserTop3Genres" should {
    "parse top genres" in {
      val f = new UserTop3Genres(317, "user_top3_genres")
      f.parse(baseSample())
      assert(f.raw_list.toSeq === Seq("animation", "comedy"))
    }
    "handle empty" in {
      val s = baseSample(); s.user_top3_genres.clear()
      val f = new UserTop3Genres(317, "user_top3_genres")
      f.parse(s)
      assert(f.raw_list.isEmpty)
    }
  }

  "UserWatchSameGenre" should {
    "return 2 when current genres overlap with recent" in {
      val f = new UserWatchSameGenre(351, "user_watch_same_genre")
      f.parse(baseSample())
      assert(f.feature_list.head === 2)
    }
    "return 1 when no overlap" in {
      val s = baseSample(); s.movie_genres = ArrayBuffer("horror", "thriller")
      val f = new UserWatchSameGenre(351, "user_watch_same_genre")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
  }

  "UserWatchSameGenre1Day" should {
    "check 1-day genre overlap" in {
      val s = baseSample(); s.movie_genres = ArrayBuffer("action")
      val f = new UserWatchSameGenre1Day(352, "user_watch_same_genre_1day")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }
    "return 1 when no 1-day overlap" in {
      val s = baseSample(); s.movie_genres = ArrayBuffer("horror")
      val f = new UserWatchSameGenre1Day(352, "user_watch_same_genre_1day")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
  }

  "UserSameGenreAvgRate" should {
    "compute avg of matching genres" in {
      val f = new UserSameGenreAvgRate(356, "user_same_genre_avg_rate")
      f.parse(baseSample())
      val avg = (3.5F + 4.0F) / 2
      assert(f.feature_list.head >= 3)
    }
    "handle empty genres with default 3" in {
      val s = baseSample(); s.movie_genres.clear()
      val f = new UserSameGenreAvgRate(356, "user_same_genre_avg_rate")
      f.parse(s)
      assert(f.feature_list.head === 3)
    }
  }

  "UserSameGenreAvgRateContinue" should {
    "compute continuous avg" in {
      val f = new UserSameGenreAvgRateContinue(357, "user_same_genre_avg_rate_continue")
      f.parse(baseSample())
      val expected = (3.5F + 4.0F) / 2
      assert(f.value_list.head === expected)
    }
    "handle empty genres with default 3.0" in {
      val s = baseSample(); s.movie_genres.clear()
      val f = new UserSameGenreAvgRateContinue(357, "user_same_genre_avg_rate_continue")
      f.parse(s)
      assert(f.value_list.head === 3.0F)
    }
  }

  "User features field types" should {
    "have correct types" in {
      assert(new UserAge(2, "user_age").field_type === FieldType.Categorical)
      assert(new UserGender(3, "user_gender").field_type === FieldType.Categorical)
      assert(new UserRateStdContinue(18, "user_rate_std_continue").field_type === FieldType.Continuous)
      assert(new UserAvgRateContinue(23, "user_avg_rate_continue").field_type === FieldType.Continuous)
      assert(new UserSameGenreAvgRateContinue(357, "user_same_genre_avg_rate_continue").field_type === FieldType.Continuous)
    }
  }
}
