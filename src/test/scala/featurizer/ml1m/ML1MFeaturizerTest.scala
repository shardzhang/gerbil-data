package featurizer.ml1m
import org.scalatest.{Matchers, WordSpec}
import org.tensorflow.example.Example
import featurizer.ml1m.ML1MSample

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class ML1MFeaturizerTest extends WordSpec with Matchers {

  private def createSample(): ML1MSample = {
    val s = new ML1MSample()
    s.user_id = "1"
    s.item_id = "1"
    s.gender = "M"
    s.age = "25"
    s.occupation = "12"
    s.zip_code = "10001"
    s.movie_title = "Toy Story (1995)"
    s.movie_genres = ArrayBuffer("animation", "children's", "comedy")
    s.movie_genre_cnt = 3
    s.movie_rate_count = 1000
    s.movie_avg_rate = 4.2
    s.movie_hot_rank = 50
    s.movie_publish_year = 1995
    s.rating = 4.0F
    s.target = 1
    s.time_hour = 14
    s.time_area = 3
    s.week_day = 3
    s.user_rate_std = 1.5F
    s.user_rate_cnt = 50
    s.user_avg_rate = 3.5F
    s.user_movie_rates = ArrayBuffer((1, 4), (2, 3), (3, 5))
    s.user_genres_rates = ArrayBuffer(("animation", 3.5F), ("comedy", 4.0F))
    s.user_genres_rate_cnts = ArrayBuffer(("animation", 5), ("comedy", 3))
    s.user_top3_genres = ArrayBuffer(("animation", 5), ("comedy", 3))
    s.user_active_day = 30
    s
  }

  "ML1MFeaturizer" should {
    "setup correctly with all features registered" in {
      val encoder = new ML1MFeaturizer().setup()
      val fields = encoder.getFieldInfo()

      assert(fields.exists(_._2 == 2))   // user_age
      assert(fields.exists(_._2 == 3))   // user_gender
      assert(fields.exists(_._2 == 4))   // user_occupation
      assert(fields.exists(_._2 == 102)) // movie_title
      assert(fields.exists(_._2 == 103)) // movie_genres
      assert(fields.exists(_._2 == 201)) // context_time_hour
      assert(fields.exists(_._2 == 301)) // user_movie_rate

      assert(encoder.raw_cate_features.nonEmpty)
      assert(encoder.raw_conti_features.nonEmpty)
    }

    "encode a sample to TFRecord Example" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      val builder = Example.newBuilder()
      encoder.encode(sample, 1L << 20, builder)
      val example = builder.build()

      val featMap = example.getFeatures.getFeatureMap.asScala
      assert(featMap.contains("target"))
      assert(featMap.contains("user_age_raw"))
      assert(featMap.contains("user_age_index"))
      assert(featMap.contains("user_age_value"))
      assert(featMap.contains("user_gender_raw"))
      assert(featMap.contains("user_gender_index"))
      assert(featMap.contains("user_gender_value"))

      val targetFeat = featMap("target")
      assert(targetFeat.getFloatList.getValue(0) === 1.0F) // target = item_id = 1
    }

    "encode user_age correctly" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()
      sample.age = "25"

      val builder = Example.newBuilder()
      encoder.encode(sample, 1L << 20, builder)
      val example = builder.build()
      val featMap = example.getFeatures.getFeatureMap.asScala

      val indexFeat = featMap("user_age_index")
      val indices = indexFeat.getInt64List.getValueList.asScala.toSeq
      // First value is 0 (R:), then age=25
      assert(indices.size === 2)
      assert(indices(0) === 0L)
    }

    "encode movie_title as multi-value feature" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      val builder = Example.newBuilder()
      encoder.encode(sample, 1L << 20, builder)
      val example = builder.build()
      val featMap = example.getFeatures.getFeatureMap.asScala

      assert(featMap.contains("movie_title_raw"))
      assert(featMap.contains("movie_title_index"))
      assert(featMap.contains("movie_title_value"))

      val rawFeat = featMap("movie_title_raw")
      // Should have R: plus individual words from title
      assert(rawFeat.getBytesList.getValueCount >= 2)
    }

    "encode with pos_map and target_map" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      // First get hashes
      val hashInfo = encoder.getHashInfo(sample, 1L << 20)
      val pos_map = hashInfo.map { case (_, idx, _, _, hash, _) =>
        (idx, hash) -> (hash % 100).toInt
      }.toMap
      val target_map = Map(1 -> 0)

      val builder = Example.newBuilder()
      val (hasFeature, hasTarget) = encoder.encode(sample, 1L << 20, builder, pos_map, target_map)
      assert(hasFeature)
      assert(hasTarget)

      val example = builder.build()
      val featMap = example.getFeatures.getFeatureMap.asScala
      assert(featMap.contains("target"))
      val targetFeat = featMap("target")
      assert(targetFeat.getFloatList.getValue(0) === 0.0F) // remapped target
    }

    "get_hash returns non-empty list" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      val hashes = encoder.get_hash(sample, 1L << 20)
      assert(hashes.nonEmpty)
    }

    "getHashInfo returns detailed hash info" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      val info = encoder.getHashInfo(sample, 1L << 20)
      assert(info.nonEmpty)
      info.foreach { case (name, idx, fType, fmt, hash, value) =>
        assert(name.nonEmpty)
        assert(idx > 0)
        assert(hash >= 0)
      }
    }

    "encode to TSV format" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      val result = encoder.encode(sample, 1L << 20, ",", ":")
      assert(result.nonEmpty)
      assert(result.contains("user_age:"))
    }
  }
}
