package featurizer.ml1m
import org.scalatest.{Matchers, WordSpec}
import org.tensorflow.example.Example

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
    s.user_rate_avg = 3.5F
    s.user_movie_rate_ids = ArrayBuffer((1, 4), (2, 3), (3, 5))
    s.user_genres_rates = ArrayBuffer(("animation", 3.5F), ("comedy", 4.0F))
    s.user_genres_rate_cnts = ArrayBuffer(("animation", 5), ("comedy", 3))
    s.user_top3_genres = ArrayBuffer(("animation", 5), ("comedy", 3))
    s.user_active_day = 30
    s
  }

  "ML1MFeaturizer" should {
    "setup correctly with all features registered" in {
      val encoder = new ML1MFeaturizer().setup()
      val fields: ArrayBuffer[(String, Int)] = encoder.getFieldInfo()

      assert(fields.exists(_._2 == 2))   // user_age
      assert(fields.exists(_._2 == 3))   // user_gender
      assert(fields.exists(_._2 == 4))   // user_occupation
      assert(fields.exists(_._2 == 201)) // context_time_hour
      assert(fields.exists(_._2 == 6))   // user_rate_std
      assert(fields.exists(_._2 == 10))  // user_movie_rate_cnt

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

    "getHash returns non-empty list" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample = createSample()

      val hashes = encoder.getHash(sample, 1L << 20)
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

    // mvn test -pl . -Dtest=featurizer.ml1m.ML1MFeaturizerTest -DfailIfNoTests=false 2>&1 | tail -80
    "reproduce genre hash collision with production max_dim" in {
      val encoder = new ML1MFeaturizer().setup()
      val sample: ML1MSample = new ML1MSample()
      sample.user_id = "1"
      sample.item_id = "1"
      sample.rating = 4.0F
      sample.target = 1
      sample.time_hour = 14
      sample.time_area = 2
      sample.week_day = 3
      sample.user_genres_rate_15days = ArrayBuffer(
        ("war", 3.75F),
        ("action", 4.0F),
        ("horror", 4.0F),
        ("fantasy", 3.0F),
        ("children's", 3.6666667F),
        ("sci-fi", 4.0F),
        ("animation", 4.0F),
        ("drama", 3.8571429F),
        ("thriller", 3.8F),
        ("romance", 3.6470587F),
        ("adventure", 4.0F),
        ("western", 4.5F),
        ("mystery", 3.5F),
        ("musical", 3.75F),
        ("comedy", 3.72F),
        ("film-noir", 4.0F)
      )

      val max_dim = 1L << 60
      val hashInfo: ArrayBuffer[(String, Int, Byte, String, Long, Float)] = encoder.getHashInfo(sample, max_dim)
      val genreHashes = hashInfo.filter(_._1 == "user_genres_rate_15day")

      println(s"user_genres_rate_15day entries: ${genreHashes.size}")
      val byHash: Map[Long, ArrayBuffer[(String, Int, Byte, String, Long, Float)]] = genreHashes.groupBy(_._5)
      println(s"Unique hashes: ${byHash.size}")
      println("\nHash -> Genres mapping:")
      byHash.toSeq.sortBy(_._1).foreach { case (hash, entries) =>
        val genres = entries.map(_._4).mkString(", ")
        println(s"  hash=%-5d → %s".format(hash, genres))
      }

      val collisions = byHash.filter(_._2.size > 1)
      if (collisions.isEmpty) {
        println("\nNO collisions in computeHash itself.")
      } else {
        println(s"\n*** FOUND ${collisions.size} computeHash collisions! ***")
        collisions.foreach { case (hash, entries) =>
          println(s"  hash=$hash collides: ${entries.map(_._4).mkString(", ")}")
        }
      }
    }
  }
}
