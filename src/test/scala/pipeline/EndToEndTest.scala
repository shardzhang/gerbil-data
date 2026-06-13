package pipeline

import featurizer.ml1m.ML1MSample
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import java.io.File
import java.nio.file.{Files, Paths}
import org.apache.commons.io.FileUtils

class EndToEndTest extends WordSpecLike with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var testDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDir = new File("target/e2e-test")
    FileUtils.deleteQuietly(testDir)
    testDir.mkdirs()

    spark = SparkSession.builder()
      .appName("e2e-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      FileUtils.deleteQuietly(testDir)
    } finally {
      if (spark != null) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        spark = null
      }
      super.afterAll()
    }
  }

  def resourcePath(path: String): String = {
    getClass.getResource(path).getPath
  }

  "ML1MPipeline" should {
    "load mini join_sample and parse correctly" in {
      val inputDir = new File(testDir, "input")
      inputDir.mkdirs()

      // Copy test resources to a writable location
      val srcJoin = resourcePath("/ml1m_sample/join_sample/part-00000")
      val srcItem = resourcePath("/ml1m_sample/item_feature/part-00000")
      Files.copy(Paths.get(srcJoin), new File(inputDir, "join_sample").toPath)
      Files.copy(Paths.get(srcItem), new File(inputDir, "item_feature").toPath)

      // Load movie info (same as ML1MPipeline.getMovieInfo)
      spark.read
        .option("sep", "\t")
        .csv(s"${inputDir.getAbsolutePath}/item_feature")
        .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
        .createOrReplaceTempView("movie_feature")

      val movieInfo = spark.sql("select movie_id, movie_title, movie_genres from movie_feature")
        .rdd.flatMap { r =>
          try {
            Some((r.getString(0).toInt, (r.getString(1), r.getString(2).split("\\|").map(_.trim))))
          } catch {
            case _: Exception => None
          }
        }.collect().toMap

      assert(movieInfo.size === 2)
      assert(movieInfo.contains(1))
      assert(movieInfo.contains(2))

      // Load and parse join_sample
      spark.read
        .option("sep", "\t")
        .csv(s"${inputDir.getAbsolutePath}/join_sample")
        .toDF("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "movie_feature", "user_behavior")
        .createOrReplaceTempView("join_sample")

      val samples: Array[(ML1MSample, Boolean)] = spark.sql("select * from join_sample")
        .rdd
        .map(r => ML1MSample.parseSample(r, movieInfo))
        .collect()

      assert(samples.length === 2)

      // Verify first sample fields
      val first = samples(0)
      val sample1 = first._1
      val ok1 = first._2
      assert(ok1 === true)
      assert(sample1.user_id === "1")
      assert(sample1.item_id === "1")
      assert(sample1.gender === "M")
      assert(sample1.age === "25")
      assert(sample1.occupation === "12")
      assert(sample1.zip_code === "10001")
      assert(sample1.rating === 4.0F)
      assert(sample1.movie_title === "Toy Story (1995)")
      assert(sample1.movie_genres.contains("animation"))
      assert(sample1.movie_genres.contains("children's"))
      assert(sample1.movie_genres.contains("comedy"))
      assert(sample1.movie_rate_count === 100)
      assert(sample1.movie_avg_rate === 4.2)
      assert(sample1.movie_hot_rank === 10)
      assert(sample1.movie_publish_year === 1995)
      assert(sample1.time_hour >= 0 && sample1.time_hour < 24)
      assert(sample1.week_day >= 1 && sample1.week_day <= 7)

      // Verify second sample
      val second = samples(1)
      val sample2 = second._1
      val ok2 = second._2
      assert(ok2 === true)
      assert(sample2.user_id === "2")
      assert(sample2.gender === "F")
      assert(sample2.age === "35")

      // Verify behavior sequences were parsed
      assert(sample1.user_movie_rates.nonEmpty)
      assert(sample2.user_movie_rates.nonEmpty)
    }
  }
}
