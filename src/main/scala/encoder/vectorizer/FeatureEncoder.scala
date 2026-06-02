package encoder.vectorizer

import com.google.protobuf.ByteString
import org.tensorflow.example.Example
import org.tensorflow.example.Feature
import org.tensorflow.example.Features
import org.tensorflow.example.Int64List
import org.tensorflow.example.BytesList
import org.tensorflow.example.FloatList

import scala.collection.mutable.HashMap

/**
 * @author shard zhang
 * @date 2026/6/1 20:28
 * @note
 */
class FeatureEncoder {
  // 1. 浮点特征
  def floatFeature(value: Float): Feature = {
    val floatList = FloatList.newBuilder().addValue(value).build()
    Feature.newBuilder().setFloatList(floatList).build()
  }

  // 2. 整数特征（广告/推荐最常用：userid, itemid, 标签）
  def int64Feature(value: Long): Feature = {
    val int64List = Int64List.newBuilder().addValue(value).build()
    Feature.newBuilder().setInt64List(int64List).build()
  }

  // 3. 字符串/bytes 特征（用户名、序列、json）
  def bytesFeature(value: Array[Byte]): Feature = {
    val bytesList = BytesList.newBuilder().addValue(ByteString.copyFrom(value)).build()
    Feature.newBuilder().setBytesList(bytesList).build()
  }

  // 字符串版本
  def bytesFeature(value: String): Feature = {
    bytesFeature(value.getBytes("UTF-8"))
  }

  // 构建一条 TFRecord 样本
  val example = Example.newBuilder()
    .setFeatures(
      Features.newBuilder()
        .putFeature("userId", bytesFeature("1001"))
        .putFeature("itemId", bytesFeature("2002"))
        .putFeature("rating", floatFeature(4.8f))
        .build()
    ).build()

  // 序列化成二进制（写入 TFRecord）
  val binaryBytes = example.toByteArray()
}





abstract class Target {
  var target: Double = 0.0

  def add(builder: Example.Builder)

  def add(builder: Example.Builder, target_map: HashMap[Int, Int]): Boolean
}


abstract class RawTarget[T] extends Target {
  def parse(input: T): Target

  def add(builder: Example.Builder): Unit = {
    builder.getFeaturesBuilder.putFeature(
      "target", TFRecord.flo
    )
  }
}