package utils

import com.google.protobuf.ByteString
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}
import java.nio.charset.StandardCharsets

/**
 * TFRecord 特征值写入
 *
 * @author shard zhang
 * @date 2026/6/2 14:55
 * @note 自定义TFRecord编码方法
 *
 *    TFRecordUtils.floatFeature(target)
 *
 *    builder.getFeaturesBuilder
 *      .putFeature(f_name + "_raw", TFRecordUtils.bytesVectorFeature(raw_buf.toArray))
 *      .putFeature(f_name + "_index", TFRecordUtils.int64VectorFeature(pos_buf.toArray))
 *      .putFeature(f_name + "_value", TFRecordUtils.floatVectorFeature(value_buf.toArray))
 *
 * 等价于:
 *    FloatListFeatureEncoder.encode(Seq(target))
 *
 *    builder.getFeaturesBuilder
 *      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
 *        .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
 *        .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
 */
object TFRecordUtils {
  // 1. 浮点
  // 单值特征.
  def floatFeature(value: Float): Feature = {
    val floatList = FloatList.newBuilder().addValue(value).build()
    Feature.newBuilder().setFloatList(floatList).build()
  }
  // 多值特征. 行为序列中权重值
  def floatVectorFeature(values: Array[Float]): Feature = {
    val builder = FloatList.newBuilder
    for (value <- values) {
      builder.addValue(value)
    }
    Feature.newBuilder.setFloatList(builder.build).build
  }

  // 2. 整数
  // 单值特征. userid, itemid
  def int64Feature(value: Long): Feature = {
    val int64List = Int64List.newBuilder().addValue(value).build()
    Feature.newBuilder().setInt64List(int64List).build()
  }
  // 多值特征. 行为序列中ID特征
  def int64VectorFeature(values: Array[Long]): Feature = {
    val builder = Int64List.newBuilder
    for (value <- values) {
      builder.addValue(value)
    }
    Feature.newBuilder.setInt64List(builder.build).build
  }

  // 3. bytes
  // 单值特征. title
  def bytesFeature(value: Array[Byte]): Feature = {
    val bytesList = BytesList.newBuilder().addValue(ByteString.copyFrom(value)).build()
    Feature.newBuilder().setBytesList(bytesList).build()
  }
  // 多值特征. 标签序列
  def bytesVectorFeature(values: Array[Array[Byte]]): Feature = {
    val builder = BytesList.newBuilder
    for (value <- values) {
      builder.addValue(ByteString.copyFrom(value))
    }
    Feature.newBuilder.setBytesList(builder.build).build
  }

  // 4. 字符串
  def bytesFeature(value: String): Feature = {
    bytesFeature(value.getBytes(StandardCharsets.UTF_8))
  }
  def bytesVectorFeature(values: Array[String]): Feature = {
    val res = new Array[Array[Byte]](values.length)
    for (i <- values.indices) {
      res(i) = values(i).getBytes(StandardCharsets.UTF_8)
    }
    bytesVectorFeature(res)
  }

  def main(args: Array[String]): Unit = {
    // 构建一条 TFRecord 样本
    val features = Features.newBuilder()
      .putFeature("userId", bytesFeature("1001"))
      .putFeature("itemId", bytesFeature("2002"))
      .putFeature("rating", floatFeature(4.8f))
      .putFeature("rate_list", floatVectorFeature(Array(1, 2, 4, 5)))
      .build()

    val example = Example.newBuilder()
      .setFeatures(features)
      .build()

    // 序列化成二进制（写入 TFRecord）
    val binaryBytes = example.toByteArray()
  }
}