package processing.sampling

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}
import scala.util.Random

/**
 * ML-1M ETL-layer negative sampler — Spark-based random/popular negative sampling
 */

/** ML-1M negative sampler: generates negative samples from items each user has NOT interacted with.
 *
 * Reads the join_sample (positive interactions) and for each user, samples items
 * they have NOT interacted with. The output has the same schema as join_sample,
 * so it can be fed into ML1MPipeline directly for binary-classification training.
 *
 * Each output row is a (user, item, label) triple with full feature context:
 *   - label = 0 (negative), or the original rating for positives
 *   - user_profile, movie_feature, user_behavior JSON strings (same format as join_sample)
 */
class ML1MNegativeSampler(spark: SparkSession, inputDir: String, val strategy: String)
  extends NegativeSampler[Row] {

  override def seed: Long = 42L

  private val SEP = "\t"

  override protected def extractUserId(row: Row): String = row.getString(0)

  override protected def extractCurrentItemId(row: Row): String = row.getString(1)

  override protected def buildNegativeRow(pos: Row, negId: String, negFeatures: String): Row = {
    // user_id, item_id, time_stamp, rating, day, user_profile, movie_feature, user_behavior
    Row(pos.getString(0), negId, pos.getString(2), "0", pos.getString(4), pos.getString(5), negFeatures, pos.getString(7))
  }

  private def loadUserHistory(): Map[String, Set[String]] = {
    val sql = "select user_id, collect_set(item_id) as items from clean_sample group by user_id"
    spark.sql(sql).rdd.flatMap(r =>
        try {
          Some(r.getString(0) -> r.getSeq[String](1).toSet)
        } catch {
          case e: Exception =>
            System.err.println(s"Warning: skipping malformed user history row: ${e.getMessage}")
            None
        })
      .collectAsMap().toMap
  }

  private def loadItemPool(): Array[String] = {
    spark.sql("select movie_id from item_feature")
      .rdd.flatMap(r => try {
        Some(r.getString(0))
      } catch {
        case e: Exception =>
          System.err.println(s"Warning: skipping malformed item pool row: ${e.getMessage}")
          None
      })
      .collect()
  }

  private def loadItemFeatures(): Map[String, String] = {
    val sql =
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
    spark.sql(sql)
      .rdd
      .flatMap(r =>
        try {
          Some(r.getString(0) -> r.getString(1))
        } catch {
          case e: Exception =>
            System.err.println(s"Warning: skipping malformed item feature row: ${e.getMessage}")
            None
        }
      )
      .collectAsMap().toMap
  }

  private def loadPopCounts(): Map[String, Int] = {
    val sql = "select item_id, count(distinct user_id) as cnt from clean_sample group by item_id"
    spark.sql(sql).rdd.flatMap(r =>
        try {
          Some(r.getString(0) -> r.getLong(1).toInt)
        } catch {
          case e: Exception =>
            System.err.println(s"Warning: skipping malformed pop count row: ${e.getMessage}")
            None
        })
      .collectAsMap().toMap
  }

  private def registerTables(): Unit = {
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
  }

  def run(outputDir: String, negRatio: Int): Unit = {
    registerTables()

    val userHistoryBc = spark.sparkContext.broadcast(loadUserHistory())
    val itemPoolBc = spark.sparkContext.broadcast(loadItemPool())
    val itemFeatureBc = spark.sparkContext.broadcast(loadItemFeatures())
    val needPop = strategy != "random"
    val popBc = if (needPop) {
      spark.sparkContext.broadcast(loadPopCounts())
    } else null

    green_println(s"[NegativeSampler] user history: ${userHistoryBc.value.size} users")
    green_println(s"[NegativeSampler] item pool: ${itemPoolBc.value.length} items")

    val sql =
      """
        |select user_id, item_id, time_stamp, rating, day, user_profile, movie_feature, user_behavior
        |from join_sample
      """.stripMargin
    val result: RDD[(Row, Boolean)] = spark.sql(sql).rdd
      .mapPartitions { iter =>
        val pool = itemPoolBc.value
        val history = userHistoryBc.value
        val featMap = itemFeatureBc.value
        val pop = if (needPop && popBc != null) popBc.value else Map.empty[String, Int]
        val rand = new Random(seed)
        val strat = strategy

        iter.flatMap { row =>
          val uid = extractUserId(row)
          val iid = extractCurrentItemId(row)
          val candidates = NegativeSampler.computeCandidates(uid, iid, pool, history)
          if (candidates.isEmpty) {
            Iterator((row, true))
          } else {
            val numNeg = math.min(negRatio, candidates.length)
            val chosen = NegativeSampler.selectNegativeItems(candidates, pop, numNeg, strat, rand)
            val posSeq = Seq((row, true))
            val negSeq: Seq[(Row, Boolean)] = chosen.map { negId =>
              val negFeat = featMap.getOrElse(negId, "{}")
              (buildNegativeRow(row, negId, negFeat), false)
            }
            (posSeq ++ negSeq).toIterator
          }
        }
      }

    val df = spark.createDataFrame(result.map(_._1), spark.table("join_sample").schema)
    green_println(s"[NegativeSampler] total samples: ${df.count()}")
    DataQualityChecker.check(df, "neg_sample", outputDir)

    df.write
      .mode("overwrite")
      .option("sep", SEP)
      .csv(outputDir)

    green_println(s"[NegativeSampler] output: $outputDir")
  }
}

object ML1MNegativeSampler {

  def main(args: Array[String]): Unit = {
    val opts = new Options()
    opts.addOption(null, "input", true, "Input directory")
    opts.addOption(null, "output", true, "Output directory (default: <path>/neg_sample)")
    opts.addOption(null, "strategy", true, "Sampling strategy: random, popular (default), mixed")
    opts.addOption(null, "neg_ratio", true, "neg_ratio: default 0. e.g., neg_ratio=5 means 5 negatives per positive")

    val parser = new DefaultParser()
    val cl = parser.parse(opts, args)
    val path = cl.getOptionValue("input")
    val outputPath = Option(cl.getOptionValue("output")).getOrElse(s"$path/neg_sample")
    val strategy = Option(cl.getOptionValue("strategy")).getOrElse("popular")
    val negRatio = Option(cl.getOptionValue("neg_ratio")).getOrElse("5").toInt
    green_println(s"path = $path, negRatio = $negRatio, strategy = $strategy, outputPath = $outputPath")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    try {
      val sampler = new ML1MNegativeSampler(spark, path, strategy)
      sampler.run(outputPath, negRatio)
    } finally {
      spark.stop()
    }
  }
}
