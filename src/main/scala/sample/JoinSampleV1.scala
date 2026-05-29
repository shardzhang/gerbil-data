package sample

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.LogUtils.{green_println, setLogLevel}
import scala.sys.process._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

// MovieID::Titile::Genres
case class ItemFeature(item_id: String, title: String, genres: Seq[String])

object ItemFeature {
  val schema: StructType = new StructType()
    .add("item_id", StringType, nullable = false)
    .add("title", StringType, nullable = true)
    .add("genres", ArrayType(StringType), nullable = true)

  def mkString(fea: ItemFeature, seq: String): String = {
    Seq(fea.item_id, fea.title, fea.genres.mkString(",")).mkString(seq)
  }
}

// UserID::Gender::Age::Occupation::Zip-code
case class UserProfile(user_id: String, gender: String, age: String, occupation: String, zip_code: String)

object UserProfile {
  val schema: StructType = new StructType()
    .add("user_id", StringType, nullable = false)
    .add("gender", StringType, nullable = true)
    .add("age", StringType, nullable = true)
    .add("occupation", StringType, nullable = true)
    .add("zip_code", StringType, nullable = true)

  def mkString(fea: UserProfile, seq: String): String = {
    Seq(fea.user_id, fea.gender, fea.age, fea.occupation, fea.zip_code).mkString(seq)
  }
}

/**
 * @author shard zhang
 * @date 2026/5/29 12:09
 * @note 读取原始样本, 拼接用户特征, 用户行为序列特征, 形成最终明文样本
 */
object JoinSampleV1 {
  private val INPUT_SEP = "::"
  private val DATE_FORMAT = "yyyy-MM-dd"
  private val defaultItemFeature = ItemFeature("-", "-", Seq("-"))
  private val defaultUserProfile = UserProfile("-", "-", "-", "-", "-")


  private def getUserProfile(spark: SparkSession, path: String): collection.Map[String, UserProfile] = {
    val source = spark.sparkContext.textFile(path + "/" + "users.dat")
    val userProfileMap = source
      .map(r => {
        // UserID::Gender::Age::Occupation::Zip-code
        val arr = r.split(INPUT_SEP)
        val user_id = arr(0)
        val gerder = arr(1)
        val age = arr(2)
        val occupation = arr(3)
        val zipcode = arr(4)

        (user_id, UserProfile(user_id, gerder, age, occupation, zipcode))
      }).collectAsMap()

    userProfileMap
  }

  private def getItemFeature(spark: SparkSession, path: String): collection.Map[String, ItemFeature] = {
    val source = spark.sparkContext.textFile(path + "/" + "movies.dat")
    val ItemFeatureMap = source
      .map(r => {
        // MovieID::Titile::Genres
        val arr = r.split(INPUT_SEP)
        val item_id = arr(0)
        val title = arr(1)
        val genres = arr(2).split("\\|")

        (item_id, ItemFeature(item_id, title, genres))
      }).collectAsMap()

    ItemFeatureMap
  }

  // 用户行为序列特征
  def getUserBehaviorFeature(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val fea = spark.sparkContext.textFile(path)
      .map(r => {
        val arr = r.split(INPUT_SEP)
        val user_id = arr(0)
        val list = arr(1)
        (user_id, list)
      })
      .toDF("user_id", "feature")
    fea
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(s"Usage: ${this.getClass.getSimpleName.stripSuffix("$")} <data_path> <yesterday>")
      System.exit(1)
    }
    val Array(path, yesterday) = args
    green_println(s"path = $path, yesterday = $yesterday")

    setLogLevel()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .enableHiveSupport() // 关联本地 Hive 元数据
      .getOrCreate()
    import spark.implicits._

    val itemFeatureMap = getItemFeature(spark, path)
    green_println(s"itemFeatureMap.size = ${itemFeatureMap.size}")
    itemFeatureMap.take(10).foreach(r => green_println(r.toString()))

    val userProfileMap = getUserProfile(spark, path)
    green_println(s"userProfileMap.size = ${userProfileMap.size}")
    userProfileMap.take(10).foreach(r => green_println(r.toString()))

    val rawSampleClean = spark.sparkContext
      .textFile(path + "/" + "raw_sample_clean")
      .map(r => {
        // user_id, movie_id, rating, time_stamp, day
        val arr = r.split(",")
        val user_id = arr(0)
        val item_id = arr(1)
        val rating = arr(2).toInt
        val time_stamp = arr(3).toLong
        val day = arr(4)
        val userProfile = userProfileMap.getOrElse(user_id, defaultUserProfile)
        val itemFeature = itemFeatureMap.getOrElse(item_id, defaultItemFeature)

        (user_id, item_id, rating, time_stamp, day, UserProfile.mkString(userProfile, ","), ItemFeature.mkString(itemFeature, ","))
      })
      .toDF("user_id", "item_id", "rating", "time_stamp", "day", "user_profile", "item_fea")

    green_println(s"rawSampleClean.count() = ${rawSampleClean.count()}")
    rawSampleClean.createOrReplaceTempView("label")

    val rateFeature = getUserBehaviorFeature(spark, path)
    green_println(s"rateFeature.count() = ${rateFeature.count()}")
    rateFeature.createOrReplaceTempView("user_rate")

    val sql =
      s"""
         | select label.user_id as user_id,
         |        item_id,
         |        time_stamp,
         |        rating,
         |        day,
         |        user_profile,
         |        item_fea,
         |        case when feature is null then "-" else feature end as rate_fea
         | from label
         | left join user_rate
         | on label.user_id = user_rate.user_id
         |""".stripMargin
    green_println(s"sql = ${sql}")

    val joinSample = spark.sql(sql).cache
    val d = joinSample.rdd.map(r => r.mkString("\t"))
    green_println(s"d.count() = ${d.count()}")
    d.take(10).foreach(green_println)

    // 存储
    val save_path = path + "/" + "join_sample" + "/" + yesterday
    s"hadoop fs -rm -r ${save_path}".!
    d.saveAsTextFile(save_path)
  }
}
