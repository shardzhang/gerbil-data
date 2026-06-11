package processing.sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import utils.LogUtils.{green_println, setLogLevel}

import scala.util.Random

/**
 * Generates negative training samples by sampling unobserved (user, item) pairs.
 *
 * Reads the join_sample (positive interactions) and for each user, samples items
 * they have NOT interacted with. The output has the same schema as join_sample,
 * so it can be fed into ML1MPipeline directly for binary-classification training.
 *
 * Each output row is a (user, item, label) triple with full feature context:
 *   - label = 0 (negative), or the original rating for positives
 *   - user_profile, movie_feature, user_behavior JSON strings (same format as join_sample)
 *
 * Input:
 *   join_sample/     — positive interaction samples (from ML1MJoinSample)
 *   clean_sample/   — deduplicated user-item interactions (from ML1MCleanSample)
 *   item_feature/   — movie features (from ML1MMovieStatFeature)
 *
 * Output:
 *   neg_sample/     — balanced positive + negative samples (same schema as join_sample)
 *
 * Usage: spark-submit --class processing.sampling.ML1MNegativeSampler \
 *   target/gerbil-data-...jar \
 *   <input_base_path> <neg_ratio> [--popular]
 */
object ML1MNegativeSampler {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: ML1MNegativeSampler <path> <neg_ratio> [--popular]")
    val path = args(0)
    val negRatio = args(1).toInt
    val usePopular = args.contains("--popular")
    val outputPath = s"$path/neg_sample"
    green_println(s"path = $path, negRatio = $negRatio, usePopular = $usePopular, outputPath = $outputPath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    try {
      process(spark, path, outputPath, negRatio, usePopular)
    } finally {
      spark.stop()
    }
  }

  def process(spark: SparkSession, inputDir: String, outputDir: String,
              negRatio: Int, usePopular: Boolean = false): Unit = {
    // Register input tables
    spark.read
      .option("sep", SEP)
      .csv(s"$inputDir/join_sample")
      .toDF("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "movie_feature", "user_behavior")
      .createOrReplaceTempView("join_sample")

    spark.read
      .option("sep", SEP)
      .csv(s"$inputDir/clean_sample")
      .toDF("user_id", "item_id", "rating", "time_stamp", "day")
      .createOrReplaceTempView("clean_sample")

    spark.read
      .option("sep", SEP)
      .csv(s"$inputDir/item_feature")
      .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
      .createOrReplaceTempView("item_feature")

    // Build user history: which items each user has interacted with => broadcast
    val userHistory = spark.sql(
      "select user_id, collect_set(item_id) as items from clean_sample group by user_id"
    ).rdd.flatMap(r => try {
      Some(r.getString(0) -> r.getSeq[String](1).toSet)
    } catch { case _: Exception => None })
      .collectAsMap().toMap
    val userHistoryBc = spark.sparkContext.broadcast(userHistory)
    green_println(s"[NegativeSampler] user history: ${userHistoryBc.value.size} users")

    // Build item pool: all movie IDs => broadcast
    val itemPool = spark.sql("select movie_id from item_feature")
      .rdd.flatMap(r => try { Some(r.getString(0)) } catch { case _: Exception => None })
      .collect()
    val itemPoolBc = spark.sparkContext.broadcast(itemPool)
    green_println(s"[NegativeSampler] item pool: ${itemPoolBc.value.length} items")

    // Build item feature lookup: movie_id → JSON string (same format as join_sample's movie_feature)
    val itemFeatureMap = spark.sql(
      """
        |select movie_id,
        |  to_json(named_struct(
        |    'movie_title', movie_title,
        |    'movie_genres', movie_genres,
        |    'movie_genre_cnt', cast(movie_genre_cnt as int),
        |    'movie_rate_count', cast(movie_rate_count as bigint),
        |    'movie_avg_rate', cast(movie_avg_rate as double),
        |    'movie_hot_rank', cast(movie_hot_rank as int)
        |  )) as movie_feature
        |from item_feature
      """.stripMargin
    ).rdd.flatMap(r => try {
      Some(r.getString(0) -> r.getString(1))
    } catch { case _: Exception => None })
      .collectAsMap().toMap
    val itemFeatureBc = spark.sparkContext.broadcast(itemFeatureMap)

    // Build popularity counts for weighted sampling
    val popCounts = if (usePopular) {
      val pc = spark.sql(
        "select item_id, count(distinct user_id) as cnt from clean_sample group by item_id"
      ).rdd.flatMap(r => try {
        Some(r.getString(0) -> r.getLong(1).toInt)
      } catch { case _: Exception => None })
        .collectAsMap().toMap
      spark.sparkContext.broadcast(pc)
    } else null

    val seed = 42L

    // Process positive samples and generate negatives
    val result: RDD[(Row, Boolean)] = spark.sql(
      """
        |select user_id, item_id, time_stamp, rating, day, user_profile, movie_feature, user_behavior
        |from join_sample
      """.stripMargin
    ).rdd.mapPartitions { iter =>
      val pool = itemPoolBc.value
      val history = userHistoryBc.value
      val featMap = itemFeatureBc.value
      val pop = if (usePopular && popCounts != null) popCounts.value else Map.empty[String, Int]
      val rand = new Random(seed)

      iter.flatMap { row =>
        val uid = row.getString(0)
        val iid = row.getString(1)
        val ts = row.getString(2)
        val rating = row.getString(3)
        val day = row.getString(4)
        val userProfile = row.getString(5)
        val userBehavior = row.getString(7)

        // Generate negative samples for this row
        val rated = history.getOrElse(uid, Set.empty)
        val candidates = pool.filter(id => id != iid && !rated.contains(id))
        if (candidates.isEmpty) Iterator((row, true))
        else {
          val numNeg = math.min(negRatio, candidates.length)
          val chosen = if (usePopular) {
            val totalPop = math.max(pop.values.sum.toDouble, 1.0)
            val weighted = candidates.map { id =>
              val w = math.pow(pop.getOrElse(id, 1).toDouble, 0.75)
              (id, w)
            }
            // 轮盘赌采样
            // 热门物品权重高 → 占据更大的"轮盘"面积 → 被选中的概率更高。
            // 为什么 power 是 0.75？这是 Word2Vec 论文中用到的经验值，比线性（1.0）更能打压头部热门、提升尾部，在推荐负采样中是标准做法
            (1 to numNeg).map { _ =>
              var r = rand.nextDouble() * weighted.map(_._2).sum
              weighted.find { case (_, w) => r -= w; r <= 0 } match {
                case Some((id, _)) => id
                case None => weighted.last._1
              }
            }.distinct.toSeq
          } else {
            rand.shuffle(candidates.toSeq).take(numNeg)
          }

          // Original positive row
          val posSeq = Seq((row, true))

          // Synthetic negative rows
          val negSeq = chosen.map { negId =>
            val negFeat = featMap.getOrElse(negId, "{}")
            val negRow = org.apache.spark.sql.Row(
              uid, negId, ts, "0", day, userProfile, negFeat, userBehavior
            )
            (negRow, false)
          }

          posSeq ++ negSeq
        }
      }
    }

    // Output
    val df = spark.createDataFrame(result.map(_._1), spark.table("join_sample").schema)
    green_println(s"[NegativeSampler] total samples: ${df.count()}")

    df.write
      .mode("overwrite")
      .option("sep", SEP)
      .csv(outputDir)

    green_println(s"[NegativeSampler] output: $outputDir")
  }
}
