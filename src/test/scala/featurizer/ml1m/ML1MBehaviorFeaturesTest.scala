package featurizer.ml1m

import org.scalatest.{Matchers, WordSpec}
import featurizer.core.FieldType

import scala.collection.mutable.ArrayBuffer

class ML1MBehaviorFeaturesTest extends WordSpec with Matchers {

  private def sample(): ML1MSample = {
    val s = new ML1MSample()
    s.user_movie_rates = ArrayBuffer((1, 4), (2, 3), (3, 5))
    s.user_movie_rate_1days = ArrayBuffer((10, 2))
    s.user_movie_rate_3days = ArrayBuffer.empty
    s.user_movie_rate_7days = ArrayBuffer((20, 4))
    s.user_movie_rate_15days = ArrayBuffer((30, 5), (31, 3))
    s.user_genres_rates = ArrayBuffer(("animation", 3.5F), ("comedy", 4.0F))
    s.user_genres_rate_1days = ArrayBuffer(("action", 4.0F))
    s.user_genres_rate_3days = ArrayBuffer.empty
    s.user_genres_rate_7days = ArrayBuffer(("drama", 3.0F))
    s.user_genres_rate_15days = ArrayBuffer(("comedy", 3.5F))
    s.user_genres_rate_cnts = ArrayBuffer(("animation", 5), ("comedy", 3))
    s.user_genres_rate_cnt_1days = ArrayBuffer(("action", 1))
    s.user_genres_rate_cnt_3days = ArrayBuffer.empty
    s.user_genres_rate_cnt_7days = ArrayBuffer(("drama", 2))
    s.user_genres_rate_cnt_15days = ArrayBuffer(("comedy", 1))
    s
  }

  "UserMovieRate" should {
    "parse movie rates with limit 200" in {
      val f = new UserMovieRate(101, "user_movie_rate")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("1", "2", "3"))
      assert(f.feature_list.toSeq === Seq(1L, 2L, 3L))
      assert(f.value_list.toSeq.map(_.toInt) === Seq(4, 3, 5))
    }

    "cap at 200 entries" in {
      val s = sample()
      s.user_movie_rates.clear()
      for (i <- 1 to 300) s.user_movie_rates.append((i, 4))
      val f = new UserMovieRate(101, "user_movie_rate")
      f.parse(s)
      assert(f.feature_list.size === 200)
    }
  }

  "UserMovieRate1Day" should {
    "parse 1-day movie rates" in {
      val f = new UserMovieRate1Day(101, "user_movie_rate_1day")
      f.parse(sample())
      assert(f.feature_list.toSeq === Seq(10L))
      assert(f.value_list.map(_.toInt) === Seq(2))
    }
  }

  "UserMovieRate3Day" should {
    "handle empty seq" in {
      val f = new UserMovieRate3Day(101, "user_movie_rate_3day")
      f.parse(sample())
      assert(f.raw_list.isEmpty)
    }
  }

  "UserMovieRate7Day" should {
    "parse 7-day movie rates" in {
      val f = new UserMovieRate7Day(101, "user_movie_rate_7day")
      f.parse(sample())
      assert(f.feature_list.toSeq === Seq(20L))
    }
  }

  "UserMovieRate15Day" should {
    "parse 15-day movie rates" in {
      val f = new UserMovieRate15Day(101, "user_movie_rate_15day")
      f.parse(sample())
      assert(f.feature_list.size === 2)
    }
  }

  "UserGenresRate" should {
    "parse genre rates with hash" in {
      val f = new UserGenresRate(103, "user_genres_rate")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("animation", "comedy"))
      assert(f.feature_list.size === 2)
      assert(f.feature_list.forall(_ != 0L))
      assert(f.value_list.toSeq === Seq(3.5F, 4.0F))
    }
  }

  "UserGenresRate1Day" should {
    "parse 1-day genre rates" in {
      val f = new UserGenresRate1Day(103, "user_genres_rate_1day")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("action"))
      assert(f.value_list.toSeq === Seq(4.0F))
    }
  }

  "UserGenresRate3Day" should {
    "handle empty 3-day rates" in {
      val f = new UserGenresRate3Day(103, "user_genres_rate_3day")
      f.parse(sample())
      assert(f.raw_list.isEmpty)
    }
  }

  "UserGenresRate7Day" should {
    "parse 7-day genre rates" in {
      val f = new UserGenresRate7Day(103, "user_genres_rate_7day")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("drama"))
      assert(f.value_list.toSeq === Seq(3.0F))
    }
  }

  "UserGenresRate15Day" should {
    "parse 15-day genre rates" in {
      val f = new UserGenresRate15Day(103, "user_genres_rate_15day")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("comedy"))
    }
  }

  "UserGenresRateCnts" should {
    "parse genre rate counts" in {
      val f = new UserGenresRateCnts(312, "user_genres_rate_cnts")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("animation", "comedy"))
      assert(f.value_list.toSeq.map(_.toInt) === Seq(5, 3))
    }
  }

  "UserGenresRateCnt1Days" should {
    "parse 1-day genre counts" in {
      val f = new UserGenresRateCnt1Days(313, "user_genres_rate_cnt_1days")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("action"))
      assert(f.value_list.head.toInt === 1)
    }
  }

  "UserGenresRateCnt7Days" should {
    "parse 7-day genre counts" in {
      val f = new UserGenresRateCnt7Days(315, "user_genres_rate_cnt_7days")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("drama"))
      assert(f.value_list.head.toInt === 2)
    }
  }

  "Behavior features field types" should {
    "all be categorical" in {
      assert(new UserMovieRate(101, "user_movie_rate").field_type === FieldType.Categorical)
      assert(new UserGenresRate(103, "user_genres_rate").field_type === FieldType.Categorical)
      assert(new UserGenresRateCnts(312, "user_genres_rate_cnts").field_type === FieldType.Categorical)
    }
  }
}
