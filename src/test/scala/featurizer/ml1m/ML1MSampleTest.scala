package featurizer.ml1m

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, WordSpec}

class ML1MSampleTest extends WordSpec with Matchers {

  private val schema = StructType(Seq(
    StructField("user_id", StringType),
    StructField("item_id", StringType),
    StructField("time_stamp", StringType),
    StructField("rating", StringType),
    StructField("day", StringType),
    StructField("user_profile", StringType),
    StructField("movie_feature", StringType),
    StructField("user_behavior", StringType)
  ))

  private val movie_info: Map[Int, (String, Array[String])] = Map(
    1 -> ("Toy Story (1995)", Array("Animation", "Children's", "Comedy")),
    2 -> ("Jumanji (1995)", Array("Adventure", "Children's", "Fantasy"))
  )

  private val userBehaviorJson =
    """{"user_movie_rate":"1:4:789123456,2:3:789123456"}"""

  private val validMovieFeature =
    """{"movie_title":"Toy Story (1995)","movie_genres":"Animation|Children's|Comedy","movie_genre_cnt":3,"movie_rate_count":1000,"movie_avg_rate":4.2,"movie_hot_rank":50}"""

  private val validUserProfile =
    """{"gender":"M","age":"25","occupation":"12","zip_code":"10001"}"""

  private def makeRow(timeStamp: String, rating: String, userProfile: String, movieFeature: String, userBehavior: String): Row =
    new GenericRowWithSchema(
      Array[Any]("1", "1", timeStamp, rating, "1995-01-01", userProfile, movieFeature, userBehavior),
      schema
    )

  "ML1MSample.parseSample" should {
    "parse a valid row correctly" in {
      val row = makeRow("789123456", "4", validUserProfile, validMovieFeature, userBehaviorJson)

      val (sample, success) = ML1MSample.parseSample(row, movie_info)
      assert(success === true)
      assert(sample.user_id === "1")
      assert(sample.item_id === "1")
      assert(sample.rating === 4.0F)
      assert(sample.target === 1)
      assert(sample.gender === "M")
      assert(sample.age === "25")
      assert(sample.occupation === "12")
      assert(sample.zip_code === "10001")
      assert(sample.movie_title === "Toy Story (1995)")
      assert(sample.movie_rate_count === 1000)
      assert(sample.movie_avg_rate === 4.2)
      assert(sample.movie_hot_rank === 50)
      assert(sample.movie_genres.toSeq === Seq("animation", "children's", "comedy"))
      assert(sample.movie_genre_cnt === 3)
      assert(sample.movie_publish_year === 1995)
    }

    "parse context time fields" in {
      val row = makeRow("789123456", "4", validUserProfile, validMovieFeature, userBehaviorJson)

      val (sample, _) = ML1MSample.parseSample(row, movie_info)
      assert(sample.time_hour >= 0 && sample.time_hour <= 23)
      assert(sample.time_area >= 0 && sample.time_area <= 5)
      assert(sample.week_day >= 1 && sample.week_day <= 7)
    }

    "parse user behavior sequence" in {
      // Use a timestamp larger than user behavior timestamps so dur > 0
      val row = makeRow("999999999", "4", validUserProfile, validMovieFeature,
        """{"user_movie_rate":"1:4:789123456,2:3:789123456"}""")

      val (sample, _) = ML1MSample.parseSample(row, movie_info)
      assert(sample.user_movie_rate_ids.nonEmpty)
      assert(sample.user_rate_cnt > 0)
      assert(sample.user_rate_avg > 0)
      assert(sample.user_rate_std >= 0)
      assert(sample.user_genres_rates.nonEmpty)
      assert(sample.user_top3_genres.nonEmpty)
    }

    "handle empty movie_feature" in {
      val row = makeRow("789123456", "4", validUserProfile, "{}", userBehaviorJson)

      val (sample, success) = ML1MSample.parseSample(row, movie_info)
      assert(success === true)
      // When movie_feature is "{}", movie_title stays at default ""
      assert(sample.movie_title === "")
      assert(sample.movie_genres.isEmpty)
    }

    "handle empty user_profile" in {
      val row = makeRow("789123456", "4", "{}", validMovieFeature, userBehaviorJson)

      val (sample, success) = ML1MSample.parseSample(row, movie_info)
      assert(success === true)
      // When user_profile is "{}", gender/age stay at default ""
      assert(sample.gender === "")
      assert(sample.age === "")
    }

    "handle invalid rating string" in {
      val row = makeRow("789123456", "invalid",
        """{"gender":"M"}""",
        """{"movie_title":"Toy Story (1995)","movie_genres":"Animation","movie_genre_cnt":1,"movie_rate_count":1000,"movie_avg_rate":4.2,"movie_hot_rank":50}""",
        userBehaviorJson)

      val (sample, _) = ML1MSample.parseSample(row, movie_info)
      assert(sample.rating === 0)
    }

    "handle null json strings gracefully" in {
      val row = makeRow("789123456", "4", null, null, null)

      val (sample, success) = ML1MSample.parseSample(row, movie_info)
      assert(success === true)
    }
  }
}
