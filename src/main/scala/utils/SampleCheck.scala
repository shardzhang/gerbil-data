package utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import utils.LogUtils.green_println

import scala.compat.Platform.currentTime

/**
 * @author Shard Zhang
 * @date 2022/11/7 21:27
 * @note trainsample样本检查, 统计field特征覆盖度, 特征维度, 正负样本个数
 *
 */
object SampleCheck {
    // \001：^A
    // \002：^B
    val sep1A = "\001"
    val sep3B = "\002\002\002"
    
    case class NNTrainSample(line_id: String, label: String, groupIdWithHashValues: Array[(String, String)])
    
    // group_id + \002\002\002 + hash_value
    def parseSlotAndValue(str: String): (String, String) = {
        val group_id = str.split(sep3B)(0)
        val hash_value = str.split(sep3B)(1)
        (group_id, hash_value)
    }
    
    def main(args: Array[String]): Unit = {
        val beginTime = currentTime
        
        // val yesterday = "20221101"
        // val slots = "1,2,3,5,6,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,404,405,406,407,409,410,411,412,414,415,416,417,419,420,421,422,505,506,507,508,509,510,511,512,513,514,515,516,608,609,610,611,612,613,614,615,616,617,618,619,10000,10001,10002,10003,10005,10006,10007,10100,10101,10102,10103,10104,10105,10106,10107,10200,10201,10202,10204,10205,10210,10211,10212,10214,10215,10220,10221,10222,10224,10225,10240,10241,10242,10250,10251,10252,10405,10406,10407,10408,10409,10410,10411,10412,10413,10414,10415,10416,10417,10418,10419,10420,10421,10422,10423,10424,10608,10609,10610,10611,10612,10613,10614,10615,10616,10617,10618"
        val yesterday = args(0)
        val basePath = args(1)
        val path = basePath + "/" + yesterday
        val test_slots = args(2).split(",").map(r => r.toInt)
        
        val spark = SparkSession
            .builder()
            .appName(this.getClass + "@" + yesterday)
            .enableHiveSupport()
            .getOrCreate()
        val sc = spark.sparkContext
        
        green_println(s"yesterday = ${yesterday}")
        green_println(s"basePath = ${basePath}")
        green_println(s"path = ${path}")
        green_println(s"slots = ${args(2)}")
        green_println(s"test_slots = ${test_slots.mkString(" ")}")
        
        val sampleData = sc.textFile(path).map(r => {
            val arr = r.split(sep1A)
            val line_id = arr(0)
            val label = arr(1)
            val groupid_and_hashvalues = arr.drop(3).map(r => parseSlotAndValue(r))
            
            NNTrainSample(line_id, label, groupid_and_hashvalues)
        }).persist(StorageLevel.MEMORY_ONLY)
        
        val sampleTotalCnt = sampleData.count()
        green_println(s"sampleTotalCnt = ${sampleTotalCnt}")
        
        // 统计 groupId dim 特征维度
        val groupidDimMap = sampleData
            .flatMap(r => r.groupIdWithHashValues)
            .distinct()
            .map(x => (x._1.toInt, 1))
            .reduceByKey(_ + _)
            .collectAsMap()
        green_println(s"groupidDimMap.size = ${groupidDimMap.size}")
        
        
        // 统计group_id特征覆盖度
        val groupidCntMap = sampleData
            .map(r => r.groupIdWithHashValues)
            .flatMap(r => r.map(_._1).distinct.map(e => (e.toInt, 1)))
            .reduceByKey(_ + _)
            .collectAsMap()
        green_println(s"groupidCntMap.size = ${groupidCntMap.size}")
        
        
        // 统计正样本group_id特征覆盖度
        val groupidPositiveCntMap = sampleData
            .filter(r => r.label == "1")
            .map(r => r.groupIdWithHashValues)
            .flatMap(r => r.map(_._1).distinct.map(e => (e.toInt, 1)))
            .reduceByKey(_ + _)
            .collectAsMap()
        green_println(s"groupidPositiveCntMap.size = ${groupidPositiveCntMap.size}")
        
        
        // 统计负样本group_id特征覆盖度
        val groupidNegativeCntMap = sampleData
            .filter(r => r.label == "0")
            .map(r => r.groupIdWithHashValues)
            .flatMap(r => r.map(_._1).distinct.map(e => (e.toInt, 1)))
            .reduceByKey(_ + _)
            .collectAsMap()
        green_println(s"groupidNegativeCntMap.size = ${groupidNegativeCntMap.size}")
        
        
        val title = Array(
            "group_id",
            "feature_coverage_num",
            "feature_coverage_ratio",
            "positive_feature_coverage_num",
            "negative_feature_coverage_num",
            "groupid_dim"
        ).mkString("\t")
        green_println(title)
        
        for (group_id <- test_slots) {
            val feature_coverage_num = groupidCntMap.getOrElse(group_id, -1)
            val feature_coverage_ratio = (groupidCntMap.getOrElse(group_id, -1) * 1.0 / sampleTotalCnt).formatted("%.3f")
            val positive_feature_coverage_num = groupidPositiveCntMap.getOrElse(group_id, -1)
            val negative_feature_coverage_num = groupidNegativeCntMap.getOrElse(group_id, -1)
            val groupid_dim = groupidDimMap.getOrElse(group_id, -1)
            
            val info = Array(group_id,
                feature_coverage_num,
                feature_coverage_ratio,
                positive_feature_coverage_num,
                negative_feature_coverage_num,
                groupid_dim
            ).mkString("\t")
            
            green_println(info)
        }
        
        val endTime = currentTime
        val duration = (endTime - beginTime) / 1000.0
        green_println(s"duration = ${duration} second")
        sys.exit(0)
    }
}
