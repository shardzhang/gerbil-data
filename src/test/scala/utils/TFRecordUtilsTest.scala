package utils

import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}
import org.tensorflow.example.{BytesList, Feature, FloatList, Int64List}

import scala.jdk.CollectionConverters._

class TFRecordUtilsTest extends WordSpec with Matchers {

  "TFRecordUtils.floatFeature" should {
    "create a single-value float feature" in {
      val feature = TFRecordUtils.floatFeature(3.14F)
      assert(feature.getFloatList.getValueList.asScala.toSeq.map(_.toFloat) === Seq(3.14F))
      assert(feature.getFloatList.getValueCount === 1)
    }
  }

  "TFRecordUtils.floatVectorFeature" should {
    "create a multi-value float feature" in {
      val feature = TFRecordUtils.floatVectorFeature(Array(1.0F, 2.0F, 3.0F))
      assert(feature.getFloatList.getValueList.asScala.toSeq.map(_.toFloat) === Seq(1.0F, 2.0F, 3.0F))
    }

    "handle empty array" in {
      val feature = TFRecordUtils.floatVectorFeature(Array.empty[Float])
      assert(feature.getFloatList.getValueCount === 0)
    }
  }

  "TFRecordUtils.int64Feature" should {
    "create a single-value int64 feature" in {
      val feature = TFRecordUtils.int64Feature(42L)
      assert(feature.getInt64List.getValueList.asScala.toSeq === Seq(42L))
      assert(feature.getInt64List.getValueCount === 1)
    }
  }

  "TFRecordUtils.int64VectorFeature" should {
    "create a multi-value int64 feature" in {
      val feature = TFRecordUtils.int64VectorFeature(Array(10L, 20L, 30L))
      assert(feature.getInt64List.getValueList.asScala.toSeq === Seq(10L, 20L, 30L))
    }

    "handle empty array" in {
      val feature = TFRecordUtils.int64VectorFeature(Array.empty[Long])
      assert(feature.getInt64List.getValueCount === 0)
    }
  }

  "TFRecordUtils.bytesFeature" should {
    "create a single-value bytes feature from byte array" in {
      val data = "hello".getBytes
      val feature = TFRecordUtils.bytesFeature(data)
      assert(feature.getBytesList.getValueList.asScala.toSeq.map(_.toByteArray.deep) === Seq(data.deep))
    }

    "create a single-value bytes feature from string" in {
      val feature = TFRecordUtils.bytesFeature("hello")
      assert(feature.getBytesList.getValueList.asScala.toSeq.map(_.toStringUtf8) === Seq("hello"))
    }
  }

  "TFRecordUtils.bytesVectorFeature" should {
    "create a multi-value bytes feature from byte arrays" in {
      val data = Array("hello".getBytes, "world".getBytes)
      val feature = TFRecordUtils.bytesVectorFeature(data)
      assert(feature.getBytesList.getValueList.asScala.toSeq.map(_.toByteArray.deep) === data.map(_.deep).toSeq)
    }

    "create a multi-value bytes feature from strings" in {
      val feature = TFRecordUtils.bytesVectorFeature(Array("hello", "world"))
      assert(feature.getBytesList.getValueList.asScala.toSeq.map(_.toStringUtf8) === Seq("hello", "world"))
    }

    "handle empty byte array array" in {
      val feature = TFRecordUtils.bytesVectorFeature(Array.empty[Array[Byte]])
      assert(feature.getBytesList.getValueCount === 0)
    }

    "handle empty string array" in {
      val feature = TFRecordUtils.bytesVectorFeature(Array.empty[String])
      assert(feature.getBytesList.getValueCount === 0)
    }
  }
}
