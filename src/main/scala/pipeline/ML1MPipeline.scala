package pipeline

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import featurizer.ml1m.ML1MSample
import featurizer.ml1m.ML1MFeaturizer
import featurizer.core.Featurizer
import utils.LogUtils.green_println
import utils.LogUtils.setLogLevel

/**
 * ML1M dataset data driver. Generates TFRecord samples plus encoding maps (json/bin).
 */
object ML1MPipeline extends Pipeline[ML1MSample] {
  override val max_dim: Long = 1L << 60

  /** External feature config path; set by --feature_config in main(). */
  var featureConfigPath: Option[String] = None

  override lazy val feature_encoder: Featurizer[ML1MSample] = {
    new ML1MFeaturizer(featureConfigPath).setup()
  }

  /** Load ML1M join_sample CSV, parse with movie info, and return RDD of samples. */
  override def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(ML1MSample, Boolean)] = {
    val movie_info = getMovieInfo(spark, inputDir)
    green_println(s"movie_info.size: ${movie_info.size}")

    spark.read
      .option("sep", "\t")
      .csv(s"${inputDir}/join_sample")
      .toDF("user_id", "item_id", "time_stamp", "rating", "day", "user_profile", "movie_feature", "user_behavior")
      .createOrReplaceTempView("join_sample")

    spark.sql("select * from join_sample")
      .rdd
      .repartition(parts)
      .map(r => ML1MSample.parseSample(r, movie_info))
  }

  /** Extract target label from the sample's target field. */
  override def getSampleTarget(sample: ML1MSample): Int = sample.target

  /** Extract timestamp (millis) from sample for time-based split. */
  override def getSampleTimestamp(sample: ML1MSample): Long = sample.time_stamp

  /** Load movie metadata (title, genres) from item_feature CSV, keyed by movie_id. */
  def getMovieInfo(spark: SparkSession, path: String): collection.Map[Int, (String, Array[String])] = {
    // item_feature.csv
    spark.read
      .option("sep", "\t")
      .csv(s"$path/item_feature")
      .toDF("movie_id", "movie_title", "movie_genres", "movie_genre_cnt", "movie_rate_count", "movie_avg_rate", "movie_hot_rank")
      .createOrReplaceTempView("movie_feature")

    val sql =
      s"""
         | select movie_id, movie_title, movie_genres
         | from movie_feature
         | """.stripMargin
    green_println(s"sql:${sql}")

    val app_mapping = spark.sql(sql).rdd.flatMap(r => {
        try {
          Some(r.getString(0).toInt -> (r.getString(1), r.getString(2).split("\\|").map(_.trim)))
        } catch {
          case _: Exception => None
        }
      })
      .collectAsMap()
    app_mapping
  }

  /** CLI entry point: parse args, restore previous maps, run pipeline, save results. */
  def main(args: Array[String]): Unit = {
    val opts = new Options()
    opts.addOption(null, "feature_threshold", true, "The statistical significance threshold")
    opts.addOption(null, "target_threshold", true, "The statistical significance threshold")
    opts.addOption(null, "sample_ratio", true, "The second sample ratio")
    opts.addOption(null, "input_dir", true, "The base dir of input data")
    opts.addOption(null, "output_dir", true, "The base dir of output path")
    opts.addOption(null, "parts", true, "The TrainingNumPartitions")
    opts.addOption(null, "yesterday", true, "The date")
    opts.addOption(null, "output_format", true, "Output format: tfrecord, parquet, both (default: tfrecord)")
    opts.addOption(null, "train_ratio", true, "Fraction of data for training (default: 0.8)")
    opts.addOption(null, "val_ratio", true, "Fraction of data for validation (default: 0.1)")
    opts.addOption(null, "feature_config", true, "Path to external feature config YAML (default: classpath /ml1m/features.yaml)")

    val parser = new DefaultParser()
    val cl = parser.parse(opts, args)
    featureConfigPath = Option(cl.getOptionValue("feature_config"))
    val feature_threshold = cl.getOptionValue("feature_threshold").toInt
    val target_threshold = cl.getOptionValue("target_threshold").toInt
    val sample_ratio = cl.getOptionValue("sample_ratio").toDouble
    val input_dir = cl.getOptionValue("input_dir")
    val output_dir = cl.getOptionValue("output_dir")
    val parts = cl.getOptionValue("parts").toInt
    val yesterday = cl.getOptionValue("yesterday")
    val output_format = Option(cl.getOptionValue("output_format")).getOrElse("tfrecord")
    val train_ratio = Option(cl.getOptionValue("train_ratio")).map(_.toDouble).getOrElse(0.8)
    val val_ratio = Option(cl.getOptionValue("val_ratio")).map(_.toDouble).getOrElse(0.1)

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    try {
      val outputPath = new Path(output_dir + "/" + yesterday)
      val fs = FileSystem.get(outputPath.toUri, hadoopConf)
      if (fs.exists(outputPath)) {
        green_println(outputPath.toString + " exists and delete.")
        fs.delete(outputPath, true)
      }
      val (pos_map_before, target_map_before, pos_dim_before) = posMapSerDe.restore(output_dir, yesterday)
      val (pos_map_after, target_map_after, pos_dim_after) = run(
        spark, yesterday, feature_threshold, target_threshold, sample_ratio,
        pos_map_before, target_map_before, pos_dim_before, input_dir, output_dir, parts, output_format,
        train_ratio, val_ratio
      )
      posMapSerDe.save(output_dir, yesterday, pos_map_after, target_map_after, pos_dim_after)
      val successPath = new Path(s"${outputPath.toString}/_SUCCESS")
      if (fs.exists(successPath)) {
        fs.delete(successPath, true)
      }
      fs.create(successPath).close()
    } finally {
      spark.stop()
    }
  }
}

/**
================================================================================
DATA QUALITY REPORT
================================================================================
────────────────────────────────────────
 Stage: train_split
 Total:      800167
 ────────────────────────────────────────
 Stage: val_split
 Total:      100020
 ────────────────────────────────────────
 Stage: test_split
 Total:      100022
 ────────────────────────────────────────
 Stage: train_stats
 Total:      800167
 Valid:      800167 (100.00%)
 Targets:    3662 distinct
 Top-5:      2858=2902, 260=2516, 1196=2516, 1210=2457, 589=2284
================================================================================
 */