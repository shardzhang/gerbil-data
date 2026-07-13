package pipeline

import com.alibaba.fastjson.JSON
import featurizer.Featurizer
import featurizer.alictr.{AliCtrFeaturizer, AliCtrSample}
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.LogUtils.{green_println, setLogLevel}

import java.util.concurrent.ThreadLocalRandom

object AliCtrPipeline extends Pipeline[AliCtrSample] {
  override val max_dim: Long = 1L << 60

  var featureConfigPath: Option[String] = None
  var targetMode: String = "binary"

  override def featurizer: Featurizer[AliCtrSample] = {
    new AliCtrFeaturizer(featureConfigPath, targetMode).setup()
  }

  override def loadTrainingSamples(spark: SparkSession, inputDir: String, parts: Int): RDD[(AliCtrSample, Boolean)] = {
    spark.read
      .option("sep", "\t")
      .csv(s"${inputDir}/join_sample")
      .toDF("user_id", "item_id", "time_stamp", "clk", "pid", "item_feature", "user_profile", "user_behavior")
      .createOrReplaceTempView("join_sample")

    spark.sql("select * from join_sample")
      .rdd
      .repartition(parts)
      .map { r =>
        val userId = r.getString(0)
        val itemId = r.getString(1)
        val timeStamp = try { r.getString(2).toLong } catch { case _: Exception => 0L }
        val clk = try { r.getString(3).toInt } catch { case _: Exception => 0 }
        val pid = if (r.isNullAt(4)) "" else r.getString(4)
        val itemFeatureJson = if (r.isNullAt(5)) "" else r.getString(5)
        val userProfileJson = if (r.isNullAt(6)) "" else r.getString(6)
        val userBehaviorJson = if (r.isNullAt(7)) "" else r.getString(7)

        var cateId = ""; var campaignId = ""; var customer = ""; var brand = ""; var price = 0.0
        if (itemFeatureJson != null && itemFeatureJson != "{}" && itemFeatureJson != "") {
          try {
            val feat = JSON.parseObject(itemFeatureJson)
            cateId = feat.getString("cate_id")
            campaignId = feat.getString("campaign_id")
            customer = feat.getString("customer")
            brand = feat.getString("brand")
            price = feat.getDoubleValue("price")
          } catch { case _: Exception => }
        }

        var cmsSegid = ""; var cmsGroupId = ""; var gender = ""; var ageLevel = ""
        var pvalueLevel = ""; var shoppingLevel = ""; var occupation = ""; var newUserClassLevel = ""
        if (userProfileJson != null && userProfileJson != "{}" && userProfileJson != "") {
          try {
            val prof = JSON.parseObject(userProfileJson)
            cmsSegid = prof.getString("cms_segid")
            cmsGroupId = prof.getString("cms_group_id")
            gender = prof.getString("gender")
            ageLevel = prof.getString("age_level")
            pvalueLevel = prof.getString("pvalue_level")
            shoppingLevel = prof.getString("shopping_level")
            occupation = prof.getString("occupation")
            newUserClassLevel = prof.getString("new_user_class_level")
          } catch { case _: Exception => }
        }

        var userHistorySeq = ""
        if (userBehaviorJson != null && userBehaviorJson != "{}" && userBehaviorJson != "") {
          try {
            val bhv = JSON.parseObject(userBehaviorJson)
            userHistorySeq = bhv.getString("user_ad_seq")
          } catch { case _: Exception => }
        }

        val (sample, success) = AliCtrSample.parseSample(
          userId, itemId, clk, timeStamp, pid,
          cateId, campaignId, customer, brand, price,
          cmsSegid, cmsGroupId, gender, ageLevel, pvalueLevel, shoppingLevel,
          occupation, newUserClassLevel, userHistorySeq)
        (sample, success)
      }
  }

  override def parseTarget(sample: AliCtrSample): Int = {
    targetMode match {
      case "binary" => sample.label
      case "multi"  => sample.target
      case _        => throw new IllegalArgumentException(s"Unknown target_mode: '$targetMode'")
    }
  }

  override def useTargetMap: Boolean = targetMode == "multi"

  override def keepSample(sample: AliCtrSample, sample_ratio: Double = 1.0): Boolean = {
    targetMode match {
      case "binary" => sample.label != 0 || ThreadLocalRandom.current().nextDouble() <= sample_ratio
      case "multi"  => true
      case _        => throw new IllegalArgumentException(s"Unknown target_mode: '$targetMode'")
    }
  }

  override def parseTimestamp(sample: AliCtrSample): Long = sample.time_stamp

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
    opts.addOption(null, "feature_config", true, "Path to external feature config YAML (default: classpath /alictr/alictr_features.yaml)")
    opts.addOption(null, "target_mode", true, "Target mode: 'binary' (clk 0/1, default) or 'multi' (adgroup_id)")

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
      val (pos_map_before, target_map_before, pos_dim_before) = vocabulary.restore(output_dir, yesterday)
      val (pos_map_after, target_map_after, pos_dim_after) = run(spark, yesterday, feature_threshold, target_threshold, sample_ratio, pos_map_before, target_map_before, pos_dim_before, input_dir, train_ratio, val_ratio, parts, output_dir, output_format)
      vocabulary.save(output_dir, yesterday, pos_map_after, target_map_after, pos_dim_after)
      val successPath = new Path(s"${outputPath.toString}/_SUCCESS")
      if (fs.exists(successPath)) fs.delete(successPath, true)
      fs.create(successPath).close()
    } finally {
      spark.stop()
    }
  }
}
