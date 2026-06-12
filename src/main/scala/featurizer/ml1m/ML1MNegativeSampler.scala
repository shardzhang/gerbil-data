package featurizer.ml1m

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random
import featurizer.core.NegativeSampler
import utils.LogUtils.green_println

/**
 * @author shard zhang
 * @date 2026/6/11 11:18
 * @note ML-1M featurizer-layer negative sampler — broadcast-based random/popular sampling
 */

/** ML-1M negative sampler: generates negative samples from items each user has NOT interacted with.
 *
 * Strategy "random":  uniform random sampling from unobserved items
 * Strategy "popular": popularity-weighted sampling (more rated → more likely as negative)
 *
 * All reference data (item pool, user history, item features) is loaded once and broadcast to executors.
 * The sampling algorithm is delegated to [[NegativeSampler]] companion object methods.
 */
class ML1MNegativeSampler(spark: SparkSession, inputDir: String) extends NegativeSampler[ML1MSample, String] {

  override def strategy: String = "popular"

  // Broadcast reference data once
  private lazy val itemPoolBc: Broadcast[Array[String]] = {
    val pool = loadItemPool()
    spark.sparkContext.broadcast(pool)
  }
  private lazy val userHistoryBc: Broadcast[Map[String, Set[String]]] = {
    val h = loadUserHistory()
    spark.sparkContext.broadcast(h)
  }
  private lazy val itemFeaturesBc: Broadcast[Map[String, (String, String, Long, Double, Int)]] = {
    val f = loadItemFeatures()
    spark.sparkContext.broadcast(f)
  }
  private lazy val popCountsBc: Broadcast[Map[String, Int]] = {
    val p = loadPopCounts()
    spark.sparkContext.broadcast(p)
  }

  private def loadItemPool(): Array[String] = {
    val df = spark.read
      .option("sep", "\t")
      .csv(s"$inputDir/item_feature")
      .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
    val pool = df.select("movie_id")
      .rdd.flatMap(r => try {
        Some(r.getString(0))
      } catch {
        case e: Exception =>
          System.err.println(s"Warning: skipping malformed item pool row: ${e.getMessage}")
          None
      })
      .collect()
    green_println(s"[NegativeSampler] item pool: ${pool.length}")
    pool
  }

  private def loadUserHistory(): Map[String, Set[String]] = {
    val df = spark.read
      .option("sep", "\t")
      .csv(s"$inputDir/clean_sample")
      .toDF("user_id", "item_id", "rating", "time_stamp", "day")
    df.select("user_id", "item_id")
      .rdd.flatMap(r => try {
        Some(r.getString(0) -> r.getString(1))
      } catch {
        case e: Exception =>
          System.err.println(s"Warning: skipping malformed user history row: ${e.getMessage}")
          None
      })
      .aggregateByKey(Set.empty[String])(_ + _, _ ++ _)
      .collectAsMap().toMap
  }

  private def loadItemFeatures(): Map[String, (String, String, Long, Double, Int)] = {
    val df = spark.read
      .option("sep", "\t")
      .csv(s"$inputDir/item_feature")
      .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
    df.rdd.flatMap { r =>
      try {
        val id = r.getString(0)
        val title = Option(r.getString(1)).getOrElse("")
        val genres = Option(r.getString(2)).getOrElse("")
        val cnt = try {
          r.getString(3).toLong
        } catch {
          case e: Exception =>
            System.err.println(s"Warning: malformed movie_rate_count: ${e.getMessage}")
            0L
        }
        val avg = try {
          r.getString(4).toDouble
        } catch {
          case e: Exception =>
            System.err.println(s"Warning: malformed movie_avg_rate: ${e.getMessage}")
            0.0
        }
        val rank = try {
          r.getString(6).toInt
        } catch {
          case e: Exception =>
            System.err.println(s"Warning: malformed movie_hot_rank: ${e.getMessage}")
            99999
        }
        Some(id -> (title, genres, cnt, avg, rank))
      } catch {
        case e: Exception =>
          System.err.println(s"Warning: skipping malformed item feature row: ${e.getMessage}")
          None
      }
    }.collectAsMap().toMap
  }

  private def loadPopCounts(): Map[String, Int] = {
    if (strategy == "random") return Map.empty
    spark.read
      .option("sep", "\t")
      .csv(s"$inputDir/clean_sample")
      .toDF("user_id", "item_id", "rating", "time_stamp", "day")
      .groupBy("item_id").count()
      .rdd.flatMap(r => try {
        Some(r.getString(0) -> r.getLong(1).toInt)
      } catch {
        case e: Exception =>
          System.err.println(s"Warning: skipping malformed pop count row: ${e.getMessage}")
          None
      })
      .collectAsMap().toMap
  }

  override protected def isPositive(sample: ML1MSample): Boolean = sample.rating >= 3

  override protected def extractUserId(sample: ML1MSample): String = sample.user_id

  override def sample(samples: RDD[(ML1MSample, Boolean)], negRatio: Int): RDD[(ML1MSample, Boolean)] = {
    if (negRatio <= 0) return samples

    val poolBc = itemPoolBc
    val historyBc = userHistoryBc
    val featBc = itemFeaturesBc
    val popBc = popCountsBc
    val strat = strategy

    val negSamples: RDD[(ML1MSample, Boolean)] = samples
      .filter { case (s, _) => isPositive(s) }
      .mapPartitions { posIter =>
        val pool = poolBc.value
        val history = historyBc.value
        val features = featBc.value
        val popCounts = popBc.value
        val randLocal = new Random(seed)

        posIter.flatMap {
          case (pos, _) =>
            val uid = extractUserId(pos)
            val candidates = NegativeSampler.computeCandidates(uid, pool, history)
            if (candidates.isEmpty) {
              Iterator.empty
            } else {
              val numNeg = math.min(negRatio, candidates.length)
              val chosen = NegativeSampler.selectNegativeItems(candidates, popCounts, numNeg, strat, randLocal)
              chosen.map { negId => (buildNegative(pos, negId, features), true) }
            }
        }
      }
    samples.union(negSamples)
  }

  /** Builds a synthetic negative sample by copying user/context/behavior from the positive sample
   * and setting item features from the pre-loaded lookup map. */
  private def buildNegative(pos: ML1MSample, negId: String, features: Map[String, (String, String, Long, Double, Int)]): ML1MSample = {
    val neg = new ML1MSample()

    // User fields
    neg.user_id = pos.user_id
    neg.gender = pos.gender;
    neg.age = pos.age
    neg.occupation = pos.occupation;
    neg.zip_code = pos.zip_code

    // Negative item
    neg.item_id = negId
    neg.target = 0;
    neg.rating = 0.0F;
    neg.label = 0

    // Context
    neg.time_hour = pos.time_hour;
    neg.time_area = pos.time_area
    neg.week_day = pos.week_day

    // User statistics (user-level, not item-dependent)
    neg.user_rate_cnt = pos.user_rate_cnt
    neg.user_rate_7day_cnt = pos.user_rate_7day_cnt
    neg.user_rate_15day_cnt = pos.user_rate_15day_cnt
    neg.user_rate_30day_cnt = pos.user_rate_30day_cnt
    neg.user_rate_std = pos.user_rate_std
    neg.user_rate_std_7day = pos.user_rate_std_7day
    neg.user_rate_std_15day = pos.user_rate_std_15day
    neg.user_rate_std_30day = pos.user_rate_std_30day
    neg.user_avg_rate = pos.user_avg_rate
    neg.user_avg_rate_7day = pos.user_avg_rate_7day
    neg.user_avg_rate_15day = pos.user_avg_rate_15day
    neg.user_avg_rate_30day = pos.user_avg_rate_30day
    neg.user_active_day = pos.user_active_day
    neg.user_reg_day = pos.user_reg_day
    neg.user_last_behavior_day = pos.user_last_behavior_day

    // Behavior sequences (user-level, not item-dependent)
    neg.user_movie_rates = pos.user_movie_rates
    neg.user_movie_rate_1days = pos.user_movie_rate_1days
    neg.user_movie_rate_3days = pos.user_movie_rate_3days
    neg.user_movie_rate_7days = pos.user_movie_rate_7days
    neg.user_movie_rate_15days = pos.user_movie_rate_15days
    neg.user_genres_rates = pos.user_genres_rates
    neg.user_genres_rate_1days = pos.user_genres_rate_1days
    neg.user_genres_rate_3days = pos.user_genres_rate_3days
    neg.user_genres_rate_7days = pos.user_genres_rate_7days
    neg.user_genres_rate_15days = pos.user_genres_rate_15days
    neg.user_genres_rate_cnts = pos.user_genres_rate_cnts
    neg.user_genres_rate_cnt_1days = pos.user_genres_rate_cnt_1days
    neg.user_genres_rate_cnt_3days = pos.user_genres_rate_cnt_3days
    neg.user_genres_rate_cnt_7days = pos.user_genres_rate_cnt_7days
    neg.user_genres_rate_cnt_15days = pos.user_genres_rate_cnt_15days
    neg.user_top3_genres = pos.user_top3_genres

    // Item features from pre-loaded lookup
    features.get(negId).foreach { case (title, genres, cnt, avg, rank) =>
      neg.movie_title = title
      neg.movie_publish_year = try {
        val p = "\\((\\d{4})\\)".r
        p.findFirstMatchIn(title).map(_.group(1).toInt).getOrElse(1990)
      } catch {
        case e: Exception =>
          System.err.println(s"Warning: failed to parse publish year from '${title}': ${e.getMessage}")
          0
      }
      neg.movie_rate_count = cnt
      neg.movie_avg_rate = avg
      neg.movie_hot_rank = rank
      if (genres.nonEmpty) {
        neg.movie_genres.clear()
        neg.movie_genres.appendAll(genres.split("\\|").map(_.trim.toLowerCase()))
        neg.movie_genre_cnt = neg.movie_genres.size
      }
    }
    neg
  }
}
