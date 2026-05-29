package sample

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.LogUtils.green_println
import utils.DateUtils.{getDate, getDay}
import scala.sys.process._

/**
 * @author shard zhang
 * @date 2026/5/29 12:09
 * @note 读取原始样本, 拼接用户特征, 用户行为序列特征, 形成最终明文样本
 */
object JoinSampleV1 {

  case class AdFeature(adgroup_id: String, cate_id: String, campaign_id: String, customer_id: String, brand: String, price: Double)

  case class UserProfile(userid: String, cms_segid: String, cms_group_id: String, final_gender_code: String, age_level: String, pvalue_level: String, shopping_level: String, occupation: String, new_user_class_level: String)

  val defaultAdFeature = AdFeature("-", "-", "-", "-", "-", 0.0D)

  val defaultUserProfile = UserProfile("-", "-", "-", "-", "-", "-", "-", "-", "-")

  def getAdFeature(spark: SparkSession, path: String): collection.Map[String, AdFeature] = {
    val source = spark.sparkContext.textFile(path + "/" + "ad_feature.csv")
    val adFeatureMap = source
      .map(r => {
        // adgroup_id,cate_id,campaign_id,customer,brand,price
        val arr = r.split(",")

        (arr(0), AdFeature(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5).toDouble))
      }).collectAsMap()
    adFeatureMap
  }

  def getUserProfile(spark: SparkSession, path: String): collection.Map[String, UserProfile] = {
    val source = spark.sparkContext.textFile(path + "/" + "user_profile.csv")
    val userProfileMap = source
      .map(r => {
        // userid,cms_segid,cms_group_id,final_gender_code,age_level,pvalue_level,shopping_level,occupation,
        val arr = r.split(",")

        val new_user_class_level = if (arr.length == 8) "-" else arr(8)
        (arr(0), UserProfile(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), new_user_class_level))
      }).collectAsMap()

    userProfileMap
  }

  // 用户行为序列特征
  def getUserBehaviorFeature(spark: SparkSession, path: String, yesterday: String, behaviorType: String): DataFrame = {
    import spark.implicits._
    val fea = spark.sparkContext.textFile(path + "/feature/" + yesterday + "/" + behaviorType)
      .map(r => {
        val arr = r.split("\t")
        val user_id = arr(0)
        val list = arr(1)

        (user_id, list)
      })
      .toDF("user_id", "feature")
    fea
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val path = args(0)
    val yesterday = args(1)
    val part = args(2).toInt
    green_println(s"path = ${path}")
    green_println(s"yesterday = ${yesterday}")
    green_println(s"part = ${part}")

    val the_day_before_yesterday = getDay(-1, yesterday)
    println(s"the_day_before_yesterday = ${the_day_before_yesterday}")

    val adFeatureMap = getAdFeature(spark, path)
    green_println(s"adFeatureMap.size = ${adFeatureMap.size}")
    adFeatureMap.take(10).foreach(r => green_println(r.toString()))

    val userProfileMap = getUserProfile(spark, path)
    green_println(s"userProfileMap.size = ${userProfileMap.size}")
    userProfileMap.take(10).foreach(r => green_println(r.toString()))

    val raw_sample_clean_day = spark.sparkContext
      .textFile(path + "/" + "raw_sample_clean" + "/" + yesterday)
      .repartition(part)
      .sample(false, 0.01)
      .map(r => {
        // user_id, item_id, time_stamp, pid, label, day
        val arr = r.split(",")

        val user_id = arr(0)
        val item_id = arr(1)
        val time_stamp = arr(2)
        val pid = arr(3)
        val label = arr(4)
        val day = arr(5)

        val userProfile = userProfileMap.getOrElse(user_id, defaultUserProfile)
        val adFeature = adFeatureMap.getOrElse(item_id, defaultAdFeature)

        val cms_segid = userProfile.cms_segid
        val cms_group_id = userProfile.cms_group_id
        val final_gender_code = userProfile.final_gender_code
        val age_level = userProfile.age_level
        val pvalue_level = userProfile.pvalue_level
        val shopping_level = userProfile.shopping_level
        val occupation = userProfile.occupation
        val new_user_class_level = userProfile.new_user_class_level

        val adgroup_id = adFeature.adgroup_id
        val cate_id = adFeature.cate_id
        val campaign_id = adFeature.campaign_id
        val customer_id = adFeature.customer_id
        val brand = adFeature.brand
        val price = adFeature.price

        val user_profile_fea = Seq(cms_segid, cms_group_id, final_gender_code, age_level, pvalue_level, shopping_level, occupation, new_user_class_level).mkString("|")
        val ad_fea = Seq(adgroup_id, cate_id, campaign_id, customer_id, brand, price).mkString("|")

        (user_id, item_id, time_stamp, pid, label, day, user_profile_fea, ad_fea)
      })
      .toDF("user_id", "item_id", "time_stamp", "pid", "label", "day", "user_profile_fea", "ad_fea")

    green_println(s"raw_sample_clean_day.count() = ${raw_sample_clean_day.count()}")
    raw_sample_clean_day.createOrReplaceTempView("label")

    val pvFeature = getUserBehaviorFeature(spark, path, the_day_before_yesterday, "pv")
    green_println(s"pvFeature.count() = ${pvFeature.count()}")
    pvFeature
      .sample(false,0.01)
      .createOrReplaceTempView("user_pv")

    val cartFeature = getUserBehaviorFeature(spark, path, the_day_before_yesterday, "cart")
    green_println(s"cartFeature.count() = ${cartFeature.count()}")
    cartFeature
      .sample(false,0.01)
      .createOrReplaceTempView("user_cart")

    val buyFeature = getUserBehaviorFeature(spark, path, the_day_before_yesterday, "buy")
    green_println(s"buyFeature.count() = ${buyFeature.count()}")
    buyFeature
      .sample(false,0.01)
      .createOrReplaceTempView("user_buy")

    val favFeature = getUserBehaviorFeature(spark, path, the_day_before_yesterday, "fav")
    green_println(s"favFeature.count() = ${favFeature.count()}")
    favFeature
      .sample(false,0.01)
      .createOrReplaceTempView("user_fav")

    val sql =
      s"""
         | select label.user_id as user_id,
         |        item_id,
         |        time_stamp,
         |        pid,
         |        label,
         |        day,
         |        user_profile_fea,
         |        ad_fea,
         |        case when user_pv.feature is null then "-" else user_pv.feature end as pv_fea,
         |        case when user_cart.feature is null then "-" else user_cart.feature end as cart_fea,
         |        case when user_buy.feature is null then "-" else user_buy.feature end as buy_fea,
         |        case when user_fav.feature is null then "-" else user_fav.feature end as fav_fea
         | from label
         | left join user_pv
         | on label.user_id = user_pv.user_id
         | left join user_cart
         | on label.user_id = user_cart.user_id
         | left join user_buy
         | on label.user_id = user_buy.user_id
         | left join user_fav
         | on label.user_id = user_fav.user_id
         |""".stripMargin
    green_println(s"sql = ${sql}")

    val joinSample = spark.sql(sql).cache
    joinSample.createOrReplaceTempView("join_sample")

    // 存储为 tfrecord 文件格式，文件内部的数据格式为 Example
    var save_path = path + "/" + "tfrecord" + "/" + yesterday
    s"hadoop fs -rm -r ${save_path}".!
    joinSample
      .repartition(part)
      .write.format("tfrecords")
      .option("recordType", "Example")
      .save(save_path)

    val d = joinSample
      .repartition(part)
      .rdd.map(r => r.mkString("\t"))
    green_println(s"d.count() = ${d.count()}")
    d.take(10).foreach(green_println)

    // 存储
    save_path = path + "/" + "join_sample" + "/" + yesterday
    s"hadoop fs -rm -r ${save_path}".!
    d.saveAsTextFile(save_path)


    /**
     * 统计特征覆盖度
     */
    val feature_stat_sql =
      s"""
         | select counts,
         |        fill_user_id,
         |        fill_item_id,
         |        fill_time_stamp,
         |        fill_pid,
         |        fill_label,
         |        fill_day,
         |
         |        fill_cms_segid,
         |        fill_cms_group_id,
         |        fill_final_gender_code,
         |        fill_age_level,
         |        fill_pvalue_level,
         |        fill_shopping_level,
         |        fill_occupation,
         |        fill_new_user_class_level,
         |
         |        fill_adgroup_id,
         |        fill_cate_id,
         |        fill_campaign_id,
         |        fill_customer_id,
         |        fill_brand,
         |        fill_price,
         |
         |        fill_pv_fea,
         |        fill_cart_fea,
         |        fill_buy_fea,
         |        fill_fav_fea,
         |
         |	    cast(fill_user_id as double) / counts as fill_user_id_ratio,
         |	    cast(fill_item_id as double) / counts as fill_item_id_ratio,
         |	    cast(fill_time_stamp as double) / counts as fill_time_stamp_ratio,
         |	    cast(fill_pid as double) / counts as fill_pid_ratio,
         |	    cast(fill_label as double) / counts as fill_label_ratio,
         |	    cast(fill_day as double) / counts as fill_day_ratio,
         |	    cast(fill_cms_segid as double) / counts as fill_cms_segid_ratio,
         |	    cast(fill_cms_group_id as double) / counts as fill_cms_group_id_ratio,
         |	    cast(fill_final_gender_code as double) / counts as fill_final_gender_code_ratio,
         |	    cast(fill_age_level as double) / counts as fill_age_level_ratio,
         |	    cast(fill_pvalue_level as double) / counts as fill_pvalue_level_ratio,
         |	    cast(fill_shopping_level as double) / counts as fill_shopping_level_ratio,
         |	    cast(fill_occupation as double) / counts as fill_occupation_ratio,
         |	    cast(fill_new_user_class_level as double) / counts as fill_new_user_class_level_ratio,
         |	    cast(fill_adgroup_id as double) / counts as fill_adgroup_id_ratio,
         |	    cast(fill_cate_id as double) / counts as fill_cate_id_ratio,
         |	    cast(fill_campaign_id as double) / counts as fill_campaign_id_ratio,
         |	    cast(fill_customer_id as double) / counts as fill_customer_id_ratio,
         |	    cast(fill_brand as double) / counts as fill_brand_ratio,
         |	    cast(fill_price as double) / counts as fill_price_ratio,
         |	    cast(fill_pv_fea as double) / counts as fill_pv_fea_ratio,
         |	    cast(fill_cart_fea as double) / counts as fill_cart_fea_ratio,
         |	    cast(fill_buy_fea as double) / counts as fill_buy_fea_ratio,
         |	    cast(fill_fav_fea as double) / counts as fill_fav_fea_ratio
         | from
         |	(select count(*) as counts,
         |            sum(case when user_id <> '-' then 1 else 0 end) as fill_user_id,
         |            sum(case when item_id <> '-' then 1 else 0 end) as fill_item_id,
         |            sum(case when time_stamp <> '-' then 1 else 0 end) as fill_time_stamp,
         |            sum(case when pid <> '-' then 1 else 0 end) as fill_pid,
         |            sum(case when label <> '-' then 1 else 0 end) as fill_label,
         |            sum(case when day <> '-' then 1 else 0 end) as fill_day,
         |
         |            sum(case when split(user_profile_fea, "\\\\|")[0] <> '-' then 1 else 0 end) as fill_cms_segid,
         |            sum(case when split(user_profile_fea, "\\\\|")[1] <> '-' then 1 else 0 end) as fill_cms_group_id,
         |            sum(case when split(user_profile_fea, "\\\\|")[2] <> '-' then 1 else 0 end) as fill_final_gender_code,
         |            sum(case when split(user_profile_fea, "\\\\|")[3] <> '-' then 1 else 0 end) as fill_age_level,
         |            sum(case when split(user_profile_fea, "\\\\|")[4] <> '-' then 1 else 0 end) as fill_pvalue_level,
         |            sum(case when split(user_profile_fea, "\\\\|")[5] <> '-' then 1 else 0 end) as fill_shopping_level,
         |            sum(case when split(user_profile_fea, "\\\\|")[6] <> '-' then 1 else 0 end) as fill_occupation,
         |            sum(case when split(user_profile_fea, "\\\\|")[7] <> '-' then 1 else 0 end) as fill_new_user_class_level,
         |
         |            sum(case when split(ad_fea, "\\\\|")[0] <> '-' then 1 else 0 end) as fill_adgroup_id,
         |            sum(case when split(ad_fea, "\\\\|")[1] <> '-' then 1 else 0 end) as fill_cate_id,
         |            sum(case when split(ad_fea, "\\\\|")[2] <> '-' then 1 else 0 end) as fill_campaign_id,
         |            sum(case when split(ad_fea, "\\\\|")[3] <> '-' then 1 else 0 end) as fill_customer_id,
         |            sum(case when split(ad_fea, "\\\\|")[4] <> '-' then 1 else 0 end) as fill_brand,
         |            sum(case when split(ad_fea, "\\\\|")[5] <> 0.0 then 1 else 0 end) as fill_price,
         |
         |			sum(case when pv_fea <> '-' then 1 else 0 end) as fill_pv_fea,
         |			sum(case when cart_fea <> '-' then 1 else 0 end) as fill_cart_fea,
         |			sum(case when buy_fea <> '-' then 1 else 0 end) as fill_buy_fea,
         |			sum(case when fav_fea <> '-' then 1 else 0 end) as fill_fav_fea
         | from join_sample
         |) T
         |""".stripMargin
    green_println(s"feature_stat_sql = ${feature_stat_sql}")

    val coverage_ratio = spark.sql(feature_stat_sql).cache().rdd.collect()
    for (s <- coverage_ratio) {
      green_println(s"s: ${s.mkString(",")}")
    }
  }
}
