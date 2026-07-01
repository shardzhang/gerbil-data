package processing.feature

import org.apache.spark.sql.SparkSession
import processing.stats.DataQualityChecker
import utils.LogUtils.{green_println, setLogLevel}

object AliCtrItemStatFeature {
  private val SEP = "\t"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage: AliCtrItemStatFeature <path>")
    val path = args(0)
    val outputPath = s"$path/item_feature"
    green_println(s"path = $path, outputPath: $outputPath")
    setLogLevel()
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      spark.read.option("header", "true").option("inferSchema", "true").csv(s"$path/ad_feature.csv")
        .createOrReplaceTempView("ad_feature")
      val sql =
        """select cast(adgroup_id as string) as adgroup_id, cast(cate_id as string) as cate_id,
          |  cast(campaign_id as string) as campaign_id, cast(customer as string) as customer,
          |  cast(brand as string) as brand, cast(price as double) as price
          |from ad_feature where adgroup_id is not null""".stripMargin
      val result = spark.sql(sql).cache()
      println(s"result.count() = ${result.count()}")
      DataQualityChecker.check(result, "item_feature", outputPath)
      result.selectExpr("adgroup_id", "cate_id", "campaign_id", "customer", "brand", "price")
        .write.mode("overwrite").option("sep", SEP).csv(outputPath)
      result.unpersist()
    } finally { spark.stop() }
  }
}
