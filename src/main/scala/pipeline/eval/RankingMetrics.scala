package pipeline.eval

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

/**
 * AUC and GAUC computation for binary classification / ranking evaluation.
 *
 * All methods are pure Scala functions with no Spark dependency,
 * making them easy to unit test and reuse in any context.
 */
object RankingMetrics {
  def localAuc(scores: Array[(Double, Double)]): Double = {
    val sorted = scores.sortBy(_._1) // 按 pctr 升序（低到高）
    val n = sorted.length
    val posCount = sorted.count(_._2 == 1.0)
    val negCount = n - posCount
    if (posCount == 0 || negCount == 0) return 0.5
    var rankSum = 0L
    for (i <- 0 until n) {
      if (sorted(i)._2 == 1.0) {
        rankSum += i // 0-indexed position
      }
    }
    (rankSum - posCount * (posCount - 1L) / 2.0) / (posCount * negCount)
  }

  def CalAuc(spark: SparkSession, scoreAndLabels: RDD[(Double, Double)], part: Int = 100): Double = {
    val metrics = new BinaryClassificationMetrics(scoreAndLabels.repartition(part))
    metrics.areaUnderROC
  }

  case class Line(
                   label: Double, pctr: Double,
                   userId: String, itemId: String, requestId: String,
                   device: String, country: String,
                   ageLevel: String, activeLevel: String
                 )

  /**
   * 从 DataFrame 构建 Line RDD
   */
  private def buildLines(spark: SparkSession, sql: String): RDD[Line] = {
    spark.sql(sql).rdd.map { r =>
      Line(
        label = r.getInt(0).toDouble,
        pctr = r.getString(1).toDouble,
        userId = r.getString(2),
        itemId = r.getString(3),
        requestId = r.getString(4),
        device = r.getString(5),
        country = r.getString(6),
        ageLevel = r.getString(7),
        activeLevel = r.getString(8)
      )
    }.cache()
  }

  /**
   * 统一 GAUC 实现：按 groupField 分组 → 每组本地算 AUC → 加权平均
   *
   * @param rowData       Line RDD
   * @param groupField    分组字段提取函数
   * @param totalClickCnt 全局点击数（用于 click 加权），传 0 则用样本数加权
   * @return GAUC
   */
  private def computeGauc(rowData: RDD[Line], groupField: Line => String, totalClickCnt: Long): Double = {
    val weightsAndAucs: Array[(Long, Double)] = rowData
      .groupBy(r => groupField(r))
      .map { case (_, items) =>
        val arr = items.map(r => (r.pctr, r.label)).toArray
        val hasPos = arr.exists(_._2 == 1.0)
        val hasNeg = arr.exists(_._2 == 0.0)
        if (hasPos && hasNeg) {
          val weight = if (totalClickCnt > 0) arr.count(_._2 == 1.0) else arr.length
          (weight.toLong, localAuc(arr))
        } else {
          (0L, 0.0)
        }
      }
      .filter(_._1 > 0)
      .collect()

    val (totalWeight, weightedSum) = weightsAndAucs.foldLeft((0L, 0.0)) {
      case ((tw, ws), (w, auc)) => (tw + w, ws + auc * w)
    }
    weightedSum / totalWeight
  }

  def OnlineAuc(spark: SparkSession, yesterday: String, sceneId: String, algoName: String): Double = {
    val sql =
      s"""
         |select label,
         |       get_json_object(others, '$$.pctr') as pctr,
         |       user_id, item_id
         |from user_ctr_label_table
         |where day = '${yesterday}'
         |and scene_id = '${sceneId}'
         |and algo = '${algoName}'
         |""".stripMargin
    val data = spark.sql(sql).rdd.map { r =>
      (r.getString(1).toDouble, r.getInt(0).toDouble)
    }
    CalAuc(spark, data)
  }

  def OfflineAuc(spark: SparkSession, path: String): Double = {
    val data = spark.sparkContext.textFile(path).map { line =>
      val parts = line.split(":|\n") // user_id:item_id:reqdt:reqid:device:country:label\nscore
      val label = parts(6).toDouble
      val pctr = parts(7).toDouble
      (pctr, label)
    }
    CalAuc(spark, data)
  }

  def OnlineGauc(spark: SparkSession, yesterday: String, sceneId: String, algoName: String, groupName: String): Double = {
    val sql =
      s"""
         |select a.label, a.pctr, a.user_id, a.item_id, a.request_id,
         |       a.device, a.country,
         |       b.age_level, b.active_level
         |from (
         |  select label,
         |         get_json_object(others, '$$.pctr') as pctr,
         |         user_id, item_id, request_id, device, country
         |  from user_ctr_label_table
         |  where day = '${yesterday}'
         |    and scene_id = '${sceneId}'
         |    and algo = '${algoName}'
         |) a
         |left join (
         |  select user_id, age_level, active_level
         |  from user_feature_table
         |  where day = date_sub('${yesterday}', 1)
         |) b on a.user_id = b.user_id
         |""".stripMargin

    val rowData = buildLines(spark, sql)
    val totalClickCnt = rowData.filter(_.label == 1).count()

    val groupFunc: Line => String = groupName match {
      case "request_id" => _.requestId
      case "user_id" => _.userId
      case "device" => _.device
      case "country" => _.country
      case "age_level" => _.ageLevel
      case "active_level" => _.activeLevel
    }
    computeGauc(rowData, groupFunc, totalClickCnt)
  }

  def OfflineGauc(spark: SparkSession, yesterday: String, path: String, groupName: String): Double = {
    val userInfoMap = spark.sql(
      s"""select user_id, age_level, active_level
         |from user_feature_table
         |where day = date_sub('${yesterday}', 1)
         |""".stripMargin
    ).rdd.map { r =>
      (r.getString(0), (r.getString(1), r.getString(2)))
    }.collectAsMap()

    // 离线日志 line: user_id:item_id:reqdt:reqid:device:country:label\nscore
    val rowData: RDD[Line] = spark.sparkContext.textFile(path).map { line =>
      val parts = line.split(":|\n")
      val (ageLevel, activeLevel) = userInfoMap.getOrElse(parts(0), ("-", "-"))
      Line(
        label = parts(6).toDouble,
        pctr = parts(7).toDouble,
        userId = parts(0),
        itemId = parts(1),
        requestId = parts(3),
        device = parts(4),
        country = parts(5),
        ageLevel = ageLevel,
        activeLevel = activeLevel
      )
    }.cache()

    val totalClickCnt = rowData.filter(_.label == 1).count()

    val groupFunc: Line => String = groupName match {
      case "request_id" => _.requestId
      case "user_id" => _.userId
      case "device" => _.device
      case "country" => _.country
      case "age_level" => _.ageLevel
      case "active_level" => _.activeLevel
    }

    computeGauc(rowData, groupFunc, totalClickCnt)
  }

  // ── main ─────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val yesterday = args(0)
    val sceneId: String = args(1)
    val algoName: String = args(2)

    val spark = SparkSession.builder()
      .appName(s"AUC_${yesterday}")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    val offlineModelPath = "hdfs://xxx"

    // AUC
    val offlineAuc = OfflineAuc(spark, offlineModelPath)
    val onlineAuc = OnlineAuc(spark, yesterday, sceneId, algoName)

    // GAUC
    val groupKeys = Seq("request_id", "user_id", "device", "country", "age_level", "active_level")
    groupKeys.foreach { key =>
      val offlineGauc = OfflineGauc(spark, yesterday, offlineModelPath, key)
      val onlineGauc = OnlineGauc(spark, yesterday, sceneId, algoName, key)
      println(s"$key | offline_gauc=$offlineGauc | online_gauc=$onlineGauc")
    }
    spark.sparkContext.stop()
  }
}
