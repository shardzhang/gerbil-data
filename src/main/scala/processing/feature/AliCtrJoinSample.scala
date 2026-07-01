package processing.feature

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object AliCtrJoinSample {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: AliCtrJoinSample <path>")
    val path = args(0)
    val savePath = s"$path/join_sample"
    green_println(s"path = $path, save_path = $savePath")
    setLogLevel()
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      spark.read.option("sep", SEP).csv(s"$path/clean_sample")
        .toDF("user_id", "item_id", "clk", "nonclk", "time_stamp", "pid").createOrReplaceTempView("clean_sample")
      spark.read.option("sep", SEP).csv(s"$path/item_feature")
        .toDF("adgroup_id", "cate_id", "campaign_id", "customer", "brand", "price").createOrReplaceTempView("item_feature")
      spark.read.option("sep", SEP).csv(s"$path/user_profile_feature")
        .toDF("up_user_id", "cms_segid", "cms_group_id", "gender", "age_level",
          "pvalue_level", "shopping_level", "occupation", "new_user_class_level").createOrReplaceTempView("user_profile")
      spark.read.option("sep", SEP).csv(s"$path/user_ad_sequence")
        .toDF("seq_user_id", "feature").createOrReplaceTempView("user_ad_seq")
      val sql = s"""select s.user_id, s.item_id, s.time_stamp, s.clk, s.pid,
        |  to_json(named_struct('cate_id', f.cate_id, 'campaign_id', f.campaign_id,
        |    'customer', f.customer, 'brand', f.brand, 'price', f.price)) as item_feature,
        |  to_json(named_struct('cms_segid', p.cms_segid, 'cms_group_id', p.cms_group_id,
        |    'gender', p.gender, 'age_level', p.age_level, 'pvalue_level', p.pvalue_level,
        |    'shopping_level', p.shopping_level, 'occupation', p.occupation,
        |    'new_user_class_level', p.new_user_class_level)) as user_profile,
        |  to_json(named_struct('user_ad_seq', coalesce(q.feature, ''))) as user_behavior
        |from clean_sample s
        |left join item_feature f on s.item_id = f.adgroup_id
        |left join user_profile p on s.user_id = p.up_user_id
        |left join user_ad_seq q on s.user_id = q.seq_user_id""".stripMargin
      val joinSample = spark.sql(sql).cache()
      green_println(f"joinSample.count(): ${joinSample.count()}")
      DataQualityChecker.check(joinSample, "join_sample", savePath)
      joinSample.selectExpr("user_id", "item_id", "time_stamp", "clk", "pid", "item_feature", "user_profile", "user_behavior")
        .write.mode("overwrite").option("sep", SEP).csv(savePath)
      joinSample.unpersist()
    } finally { spark.stop() }
  }
}
