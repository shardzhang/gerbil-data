package processing.alictr

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object AliCtrUserProfileFeature {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: AliCtrUserProfileFeature <path>")
    val path = args(0)
    val outputPath = s"$path/user_profile_feature"
    green_println(s"path = $path, outputPath: $outputPath")
    setLogLevel()
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      spark.read.option("header", "true").option("inferSchema", "true").csv(s"$path/user_profile.csv")
        .createOrReplaceTempView("user_profile")
      val sql =
        """select cast(userid as string) as user_id, cast(cms_segid as string) as cms_segid,
          |  cast(cms_group_id as string) as cms_group_id, cast(final_gender_code as string) as gender,
          |  cast(age_level as string) as age_level, cast(pvalue_level as string) as pvalue_level,
          |  cast(shopping_level as string) as shopping_level, cast(occupation as string) as occupation,
          |  cast(new_user_class_level as string) as new_user_class_level
          |from user_profile where userid is not null""".stripMargin
      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      DataQualityChecker.check(result, "user_profile_feature", outputPath)
      result.selectExpr("user_id", "cms_segid", "cms_group_id", "gender", "age_level",
        "pvalue_level", "shopping_level", "occupation", "new_user_class_level")
        .write.mode("overwrite").option("sep", SEP).csv(outputPath)
      result.unpersist()
    } finally { spark.stop() }
  }
}
