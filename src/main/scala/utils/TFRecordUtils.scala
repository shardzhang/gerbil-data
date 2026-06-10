package utils

import com.google.protobuf.ByteString
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}
import java.nio.charset.StandardCharsets

/**
 * TFRecord feature value writing
 *
 * @author shard zhang
 * @date 2026/6/2 14:55
 * @note Custom TFRecord encoding method
 *
 *    TFRecordUtils.floatFeature(target)
 *
 *    builder.getFeaturesBuilder
 *      .putFeature(f_name + "_raw", TFRecordUtils.bytesVectorFeature(raw_buf.toArray))
 *      .putFeature(f_name + "_index", TFRecordUtils.int64VectorFeature(pos_buf.toArray))
 *      .putFeature(f_name + "_value", TFRecordUtils.floatVectorFeature(value_buf.toArray))
 *
 * Equivalent to:
 *    FloatListFeatureEncoder.encode(Seq(target))
 *
 *    builder.getFeaturesBuilder
 *      .putFeature(f_name + "_raw", BytesListFeatureEncoder.encode(raw_buf.map(_.getBytes(UTF_8))))
 *        .putFeature(f_name + "_index", Int64ListFeatureEncoder.encode(pos_buf))
 *        .putFeature(f_name + "_value", FloatListFeatureEncoder.encode(value_buf))
 */
/** Helper to construct TensorFlow Feature protos (float, int64, bytes) for TFRecord writing. */
object TFRecordUtils {
  /** Single float value feature (e.g. rating, target). */
  def floatFeature(value: Float): Feature = {
    val floatList = FloatList.newBuilder().addValue(value).build()
    Feature.newBuilder().setFloatList(floatList).build()
  }
  /** Multi-value float feature (e.g. weight sequence in behavior). */
  def floatVectorFeature(values: Array[Float]): Feature = {
    val builder = FloatList.newBuilder
    for (value <- values) {
      builder.addValue(value)
    }
    Feature.newBuilder.setFloatList(builder.build).build
  }

  /** Single int64 feature (e.g. userid, itemid). */
  def int64Feature(value: Long): Feature = {
    val int64List = Int64List.newBuilder().addValue(value).build()
    Feature.newBuilder().setInt64List(int64List).build()
  }
  /** Multi-value int64 feature (e.g. ID sequence in behavior). */
  def int64VectorFeature(values: Array[Long]): Feature = {
    val builder = Int64List.newBuilder
    for (value <- values) {
      builder.addValue(value)
    }
    Feature.newBuilder.setInt64List(builder.build).build
  }

  /** Single bytes feature (e.g. title, text). */
  def bytesFeature(value: Array[Byte]): Feature = {
    val bytesList = BytesList.newBuilder().addValue(ByteString.copyFrom(value)).build()
    Feature.newBuilder().setBytesList(bytesList).build()
  }
  /** Multi-value bytes feature (e.g. label sequence). */
  def bytesVectorFeature(values: Array[Array[Byte]]): Feature = {
    val builder = BytesList.newBuilder
    for (value <- values) {
      builder.addValue(ByteString.copyFrom(value))
    }
    Feature.newBuilder.setBytesList(builder.build).build
  }

  /** Convenience: single string feature (UTF-8 encoded). */
  def bytesFeature(value: String): Feature = {
    bytesFeature(value.getBytes(StandardCharsets.UTF_8))
  }
  /** Convenience: string array encoded as multi-value bytes feature. */
  def bytesVectorFeature(values: Array[String]): Feature = {
    val res = new Array[Array[Byte]](values.length)
    for (i <- values.indices) {
      res(i) = values(i).getBytes(StandardCharsets.UTF_8)
    }
    bytesVectorFeature(res)
  }

  def main(args: Array[String]): Unit = {
    // Build a sample TFRecord with various feature types
    val features = Features.newBuilder()
      .putFeature("userId", bytesFeature("1001"))
      .putFeature("itemId", bytesFeature("2002"))
      .putFeature("rating", floatFeature(4.8f))
      .putFeature("rate_list", floatVectorFeature(Array(1, 2, 4, 5)))
      .build()

    val example = Example.newBuilder()
      .setFeatures(features)
      .build()

    // Serialize to binary bytes (ready for TFRecord writing)
    val binaryBytes = example.toByteArray()
  }
}