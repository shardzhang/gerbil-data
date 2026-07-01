package pipeline.mobilerec

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import featurizer.mobilerec.{MobileRecFeaturizer, MobileRecSample}
import featurizer.core.Featurizer
import pipeline.Pipeline
import utils.LogUtils.{green_println, setLogLevel}

import java.util.concurrent.ThreadLocalRandom

object MobileRecPipeline extends Pipeline[MobileRecSample] {
  override val max_dim: Long = 1L << 60

  var featureConfigPath: Option[String] = None
  var targetMode: String = "binary"

  override def feature_encoder: Featurizer[MobileRecSample] = {
    new MobileRecFeaturizer(featureConfigPath, targetMode).setup()
  }

  override def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(MobileRecSample, Boolean)] = {
    spark.read
      .option("sep", "\t")
      .csv(s"${inputDir}/join_sample")
      .toDF("user_id", "item_id", "time_stamp", "rating", "day", "app_category", "review", "user_behavior", "item_feature")
      .createOrReplaceTempView("join_sample")

    spark.sql("select * from join_sample")
      .rdd
      .repartition(parts)
      .map { r =>
        val userId = r.getString(0)
        val itemId = r.getString(1)
        val timeStamp = r.getString(2)
        val rating = try { r.getString(3).toFloat } catch { case _: Exception => 0.0F }
        val appCategory = if (r.isNullAt(5)) "" else r.getString(5)
        val review = if (r.isNullAt(6)) "" else r.getString(6)
        val userBehavior = if (r.isNullAt(7)) "" else r.getString(7)
        val itemFeature = if (r.isNullAt(8)) "" else r.getString(8)

        val ts = try { timeStamp.toLong } catch { case _: Exception => 0L }

        val (sample, success) = MobileRecSample.parseSampleFromLine(
          userId, itemId, rating, ts, appCategory, review, userBehavior)

        if (itemFeature != null && itemFeature != "{}" && itemFeature != "") {
          try {
            val feat = com.alibaba.fastjson.JSON.parseObject(itemFeature)
            sample.app_price = feat.getDoubleValue("price")
            sample.app_avg_rating = feat.getDoubleValue("avg_rating")
            sample.app_num_reviews = feat.getLongValue("num_reviews")
            sample.app_content_rating = feat.getString("content_rating")
          } catch {
            case _: Exception =>
          }
        }
        (sample, success)
      }
  }

  override def getSampleTarget(sample: MobileRecSample): Int = {
    targetMode match {
      case "binary" => sample.label
      case "multi"  => sample.target
      case "rating" => sample.rating.toInt
      case _        => throw new IllegalArgumentException(s"Unknown target_mode: '$targetMode'")
    }
  }

  override def useTargetMap: Boolean = targetMode == "multi"

  override def keepSample(sample: MobileRecSample, sample_ratio: Double = 1.0): Boolean = {
    targetMode match {
      case "binary" => sample.label != 0 || ThreadLocalRandom.current().nextDouble() <= sample_ratio
      case "multi"  => true
      case "rating" => true
      case _        => throw new IllegalArgumentException(s"Unknown target_mode: '$targetMode'")
    }
  }

  override def getSampleTimestamp(sample: MobileRecSample): Long = sample.time_stamp

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
    opts.addOption(null, "feature_config", true, "Path to external feature config YAML (default: classpath /mobilerec/mobilerec_features.yaml)")
    opts.addOption(null, "target_mode", true, "Target mode: 'binary' (label 0/1, default) or 'multi' (app_package)")

    val parser = new DefaultParser()
    val cl = parser.parse(opts, args)
    featureConfigPath = Option(cl.getOptionValue("feature_config"))
    targetMode = Option(cl.getOptionValue("target_mode")).getOrElse("binary")
    val feature_threshold = cl.getOptionValue("feature_threshold").toInt
    val target_threshold = cl.getOptionValue("target_threshold").toInt
    val sample_ratio = Option(cl.getOptionValue("sample_ratio")).map(_.toDouble).getOrElse(0.1)
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
      if (fs.exists(successPath)) fs.delete(successPath, true)
      fs.create(successPath).close()
    } finally {
      spark.stop()
    }
  }
}
