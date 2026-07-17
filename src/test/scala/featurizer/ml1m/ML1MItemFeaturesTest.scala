package featurizer.ml1m

import featurizer.{ContinuousFeature, FieldType}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class ML1MItemFeaturesTest extends WordSpec with Matchers {

  private def sample(): ML1MSample = {
    val s = new ML1MSample()
    s.item_id = "42"
    s.movie_title = "Toy Story (1995)"
    s.movie_genres = ArrayBuffer("animation", "children's", "comedy")
    s.movie_genre_cnt = 3
    s.movie_rate_count = 1000
    s.movie_avg_rate = 4.2
    s.movie_hot_rank = 50
    s.movie_publish_year = 1995
    s
  }

  "MovieID" should {
    "parse item_id to int" in {
      val f = new MovieID(101, "movie_id")
      f.parse(sample())
      assert(f.raw_list.head === "42")
      assert(f.feature_list.head === 42)
    }

    "handle non-numeric item_id" in {
      val s = sample()
      s.item_id = "abc"
      val f = new MovieID(101, "movie_id")
      f.parse(s)
      assert(f.feature_list.head === 0)
    }
  }

  "MovieTitle" should {
    "tokenize title and remove year" in {
      val f = new MovieTitle(102, "movie_title")
      f.parse(sample())
      val tokens = f.raw_list.toSeq
      assert(tokens.contains("Toy"))
      assert(tokens.contains("Story"))
      assert(!tokens.exists(_.contains("1995")))
    }

    "handle empty title" in {
      val s = sample()
      s.movie_title = ""
      val f = new MovieTitle(102, "movie_title")
      f.parse(s)
      assert(f.raw_list.isEmpty)
    }

    "produce hash values for features" in {
      val f = new MovieTitle(102, "movie_title")
      f.parse(sample())
      assert(f.feature_list.nonEmpty)
      f.feature_list.foreach(v => assert(v != 0L))
    }
  }

  "MovieGenres" should {
    "parse all genres" in {
      val f = new MovieGenres(103, "movie_genres")
      f.parse(sample())
      assert(f.raw_list.toSeq === Seq("animation", "children's", "comedy"))
    }

    "handle empty genres" in {
      val s = sample()
      s.movie_genres.clear()
      val f = new MovieGenres(103, "movie_genres")
      f.parse(s)
      assert(f.raw_list.isEmpty)
    }

    "skip null genre" in {
      val s = sample()
      s.movie_genres.append(null)
      val f = new MovieGenres(103, "movie_genres")
      f.parse(s)
      assert(f.raw_list.size === 3)
    }
  }

  "MovieRateCount" should {
    "bucket 0 to 1" in {
      val s = sample(); s.movie_rate_count = 0
      val f = new MovieRateCount(104, "movie_rate_count")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }

    "bucket 1-2 to 2" in {
      val s = sample(); s.movie_rate_count = 2
      val f = new MovieRateCount(104, "movie_rate_count")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }

    "bucket 1000 to 10" in {
      val s = sample(); s.movie_rate_count = 1000
      val f = new MovieRateCount(104, "movie_rate_count")
      f.parse(s)
      assert(f.feature_list.head === 10)
    }

    "bucket >1000 to 11" in {
      val s = sample(); s.movie_rate_count = 5000
      val f = new MovieRateCount(104, "movie_rate_count")
      f.parse(s)
      assert(f.feature_list.head === 11)
    }
  }

  "MovieAvgRate" should {
    "bucket 0.0 to 1" in {
      val s = sample(); s.movie_avg_rate = 0.0
      val f = new MovieRateAvg(105, "movie_avg_rate")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }

    "bucket 4.2 to 8" in {
      val f = new MovieRateAvg(105, "movie_avg_rate")
      f.parse(sample())
      assert(f.feature_list.head === 8)
    }

    "bucket >4.5 to 9" in {
      val s = sample(); s.movie_avg_rate = 4.8
      val f = new MovieRateAvg(105, "movie_avg_rate")
      f.parse(s)
      assert(f.feature_list.head === 9)
    }
  }

  "MovieAvgRateContinue" should {
    "use raw value" in {
      val f = new MovieRateAvgContinue(109, "movie_avg_rate_continue")
      f.parse(sample())
      assert(f.raw_list.head === "4.2")
      assert(f.feature_list.head === 1L)
      assert(f.value_list.head === 4.2F)
    }

    "handle exception with 0" in {
      val f = new MovieRateAvgContinue(109, "movie_avg_rate_continue")
      assert(f.isInstanceOf[ContinuousFeature[ML1MSample]])
    }
  }

  "MovieGenreCnt" should {
    "cap at 3" in {
      val f = new MovieGenreCnt(106, "movie_genre_cnt")
      f.parse(sample())
      assert(f.feature_list.head === 3)
    }

    "handle 0 genres" in {
      val s = sample(); s.movie_genres.clear()
      val f = new MovieGenreCnt(106, "movie_genre_cnt")
      f.parse(s)
      assert(f.feature_list.head === 0)
    }

    "handle 1 genre" in {
      val s = sample(); s.movie_genres.clear(); s.movie_genres.append("comedy")
      val f = new MovieGenreCnt(106, "movie_genre_cnt")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
  }

  "MovieHotRank" should {
    "bucket <=100 to 4" in {
      val s = sample(); s.movie_hot_rank = 50
      val f = new MovieHotRank(107, "item_hot_rank")
      f.parse(s)
      assert(f.feature_list.head === 4)
    }

    "bucket <=500 to 3" in {
      val s = sample(); s.movie_hot_rank = 300
      val f = new MovieHotRank(107, "item_hot_rank")
      f.parse(s)
      assert(f.feature_list.head === 3)
    }

    "bucket <=2000 to 2" in {
      val s = sample(); s.movie_hot_rank = 1500
      val f = new MovieHotRank(107, "item_hot_rank")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }

    "bucket >2000 to 1" in {
      val s = sample(); s.movie_hot_rank = 99999
      val f = new MovieHotRank(107, "item_hot_rank")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }
  }

  "MoviePublishYear" should {
    "bucket 1995 to 5" in {
      val f = new MoviePublishYear(108, "movie_publish_year")
      f.parse(sample())
      assert(f.feature_list.head === 5)
    }

    "bucket 0 to 1" in {
      val s = sample(); s.movie_publish_year = 0
      val f = new MoviePublishYear(108, "movie_publish_year")
      f.parse(s)
      assert(f.feature_list.head === 1)
    }

    "bucket <1970 to 2" in {
      val s = sample(); s.movie_publish_year = 1960
      val f = new MoviePublishYear(108, "movie_publish_year")
      f.parse(s)
      assert(f.feature_list.head === 2)
    }

    "bucket >=2010 to 7" in {
      val s = sample(); s.movie_publish_year = 2020
      val f = new MoviePublishYear(108, "movie_publish_year")
      f.parse(s)
      assert(f.feature_list.head === 7)
    }
  }

  "Item features" should {
    "have correct field types" in {
      assert(new MovieID(101, "movie_id").field_type === FieldType.Categorical)
      assert(new MovieTitle(102, "movie_title").field_type === FieldType.Categorical)
      assert(new MovieGenres(103, "movie_genres").field_type === FieldType.Categorical)
      assert(new MovieRateCount(104, "movie_rate_count").field_type === FieldType.Categorical)
      assert(new MovieRateAvg(105, "movie_avg_rate").field_type === FieldType.Categorical)
      assert(new MovieRateAvgContinue(109, "movie_avg_rate_continue").field_type === FieldType.Continuous)
      assert(new MovieGenreCnt(106, "movie_genre_cnt").field_type === FieldType.Categorical)
      assert(new MovieHotRank(107, "item_hot_rank").field_type === FieldType.Categorical)
      assert(new MoviePublishYear(108, "movie_publish_year").field_type === FieldType.Categorical)
    }
  }
}
