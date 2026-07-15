package pipeline.serde

import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ParquetRecordDataTest extends WordSpec with Matchers {

  "ParquetRecordDataBuilder" should {
    "build a ParquetRecordData with put/putAll" in {
      val record = ParquetRecordData.newBuilder()
        .put("field1", 123)
        .put("field2", "hello")
        .putAll(mutable.Map("field3" -> 45.0, "field4" -> List(1, 2, 3)))
        .build()

      assert(record.columns("field1") === 123)
      assert(record.columns("field2") === "hello")
      assert(record.columns("field3") === 45.0)
      assert(record.columns("field4") === List(1, 2, 3))
    }

    "support chain calls and clear" in {
      val builder = ParquetRecordData.newBuilder()
      builder.put("a", 1).put("b", 2)
      assert(builder.build().columns.size === 2)
      builder.clear()
      assert(builder.build().columns.size === 0)
    }
  }

  "ParquetRecordData.to_seq" should {
    "return values in column order, with null for missing" in {
      val record = ParquetRecordData.newBuilder()
        .put("name", "alice")
        .put("age", 25)
        .build()

      val seq = record.to_seq()
      assert(seq === Seq("alice", 25, null))
    }
  }

  "ParquetRecordData.from_example" should {
    "parse Example with target field" in {
      val example = Example.newBuilder()
        .setFeatures(Features.newBuilder()
          .putFeature("target", Feature.newBuilder()
            .setFloatList(FloatList.newBuilder().addValue(3.5F).build())
            .build())
          .build())
        .build()

      val record = ParquetRecordData.from_example(example)
      assert(record.columns("target") === 3.5F)
    }

    "parse Example with _raw fields" in {
      val example = Example.newBuilder()
        .setFeatures(Features.newBuilder()
          .putFeature("title_raw", Feature.newBuilder()
            .setBytesList(BytesList.newBuilder()
              .addValue(ByteString.copyFrom("hello".getBytes))
              .addValue(ByteString.copyFrom("world".getBytes))
              .build())
            .build())
          .build())
        .build()

      val record = ParquetRecordData.from_example(example)
      val raw = record.columns("title_raw").asInstanceOf[Seq[Array[Byte]]]
      assert(raw.map(new String(_)) === Seq("hello", "world"))
    }

    "parse Example with _index fields" in {
      val example = Example.newBuilder()
        .setFeatures(Features.newBuilder()
          .putFeature("item_index", Feature.newBuilder()
            .setInt64List(Int64List.newBuilder().addValue(10L).addValue(20L).build())
            .build())
          .build())
        .build()

      val record = ParquetRecordData.from_example(example)
      assert(record.columns("item_index") === Seq(10L, 20L))
    }

    "parse Example with _value fields" in {
      val example = Example.newBuilder()
        .setFeatures(Features.newBuilder()
          .putFeature("score_value", Feature.newBuilder()
            .setFloatList(FloatList.newBuilder().addValue(1.5F).addValue(2.5F).build())
            .build())
          .build())
        .build()

      val record = ParquetRecordData.from_example(example)
      assert(record.columns("score_value") === Seq(1.5F, 2.5F))
    }

    "skip fields that don't match known suffixes" in {
      val example = Example.newBuilder()
        .setFeatures(Features.newBuilder()
          .putFeature("target", Feature.newBuilder()
            .setFloatList(FloatList.newBuilder().addValue(5.0F).build())
            .build())
          .putFeature("unknown_field", Feature.newBuilder()
            .setInt64List(Int64List.newBuilder().addValue(99L).build())
            .build())
          .build())
        .build()

      val record = ParquetRecordData.from_example(example)
      assert(record.columns.contains("target"))
      assert(!record.columns.contains("unknown_field"))
    }
  }
}
