package featurizer.core

import org.scalatest.{Matchers, WordSpec}
import org.tensorflow.example.Example
import tfrecords.serde.{BytesListFeatureEncoder, FloatListFeatureEncoder, Int64ListFeatureEncoder}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class FeatureEncoderTest extends WordSpec with Matchers {

  // ==================== Concrete test implementations ====================

  private class TestRawTarget extends RawTarget[String] {
    override def parse(input: String): RawTarget[String] = {
      target = input.split(",").head.toFloat
      this
    }
  }

  private class TestCategoricalFeature(f_i: Int, f_n: String)
    extends CategoricalFeature[String](f_i, f_n) {
    override def parse(sample: String): RawFeature = {
      clear()
      val parts = sample.split(",")
      for (p <- parts) {
        val v = p.trim.toLong
        if (v != 0) {
          raw_list.append(p)
          feature_list.append(v)
          value_list.append(1.0F)
        }
      }
      this
    }
  }

  private class TestContinuousFeature(f_i: Int, f_n: String)
    extends ContinuousFeature[String](f_i, f_n) {
    override def parse(sample: String): RawFeature = {
      clear()
      val parts = sample.split(",")
      for (p <- parts) {
        val v = p.trim.toLong
        if (v != 0) {
          raw_list.append(p)
          feature_list.append(v)
          value_list.append(v.toFloat)
        }
      }
      this
    }
  }

  // ==================== RawTarget ====================

  "RawTarget" should {
    "parse and add to Example builder" in {
      val target = new TestRawTarget()
      target.parse("3.5")

      val builder = Example.newBuilder()
      target.add(builder)
      val example = builder.build()

      val feat = example.getFeatures.getFeatureMap.get("target")
      assert(feat.getFloatList.getValue(0) === 3.5F)
    }

    "add with target_map" in {
      val target = new TestRawTarget()
      target.parse("3.5")
      val target_map = Map(3 -> 1)

      val builder = Example.newBuilder()
      val result = target.add(builder, target_map)
      assert(result === true)

      val example = builder.build()
      val feat = example.getFeatures.getFeatureMap.get("target")
      assert(feat.getFloatList.getValue(0) === 1.0F)
    }

    "return false when target not in target_map" in {
      val target = new TestRawTarget()
      target.parse("3.5")
      val target_map = Map(1 -> 0)

      val builder = Example.newBuilder()
      val result = target.add(builder, target_map)
      assert(result === false)
    }

    "add to mutable map" in {
      val target = new TestRawTarget()
      target.parse("2.0")
      val map = mutable.Map.empty[String, Any]

      val result = target.add(map, null)
      assert(result === true)
      assert(map("target") === 2.0F)
    }

    "add to mutable map with target_map" in {
      val target = new TestRawTarget()
      target.parse("3.0")
      val map = mutable.Map.empty[String, Any]

      val result = target.add(map, Map(3 -> 1))
      assert(result === true)
      assert(map("target") === 1.0F)
    }
  }

  // ==================== CategoricalFeature ====================

  "CategoricalFeature" should {
    "computeHash consistently" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("42")

      val hash1 = feat.computeHash(42, 1000)
      val hash2 = feat.computeHash(42, 1000)
      assert(hash1 === hash2)
      assert(hash1 >= 0 && hash1 < 1000)
    }

    "compute different hashes for different f_index" in {
      val feat1 = new TestCategoricalFeature(1, "feat1")
      val feat2 = new TestCategoricalFeature(2, "feat2")
      feat1.parse("42")
      feat2.parse("42")

      val hash1 = feat1.computeHash(42, 1000)
      val hash2 = feat2.computeHash(42, 1000)
      assert(hash1 !== hash2)
    }

    "getHash returns list of hashes" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("10,20,30")

      val hashes = feat.getHash(1000)
      assert(hashes.size === 3)
      hashes.foreach(h => assert(h >= 0 && h < 1000))
    }

    "skip zero values in getHash" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("0,10,0,20")
      val hashes = feat.getHash(1000)
      assert(hashes.size === 2)
    }

    "add to Example builder" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("42,100")
      val builder = Example.newBuilder()

      feat.add(1000L, builder)
      val example = builder.build()
      val featMap = example.getFeatures.getFeatureMap

      assert(featMap.containsKey("test_feat_raw"))
      assert(featMap.containsKey("test_feat_index"))
      assert(featMap.containsKey("test_feat_value"))
    }

    "add with pos_map" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("42")
      val hash = feat.computeHash(42, 1000)
      val pos_map = Map((1, hash) -> 5)

      val builder = Example.newBuilder()
      val result = feat.add(1000L, builder, pos_map)
      assert(result === true)

      val example = builder.build()
      val indexFeat = example.getFeatures.getFeatureMap.get("test_feat_index")
      assert(indexFeat.getInt64List.getValueList.asScala.toSeq === Seq(0L, 5L))
    }

    "return false when hash not in pos_map" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("42")
      val pos_map = Map((1, 999L) -> 5)

      val builder = Example.newBuilder()
      val result = feat.add(1000L, builder, pos_map)
      assert(result === false)
    }

    "add to encoded_map" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("10,20")
      val encoded_map = mutable.HashMap.empty[String, ArrayBuffer[Long]]

      feat.add(1000L, encoded_map)
      assert(encoded_map("test_feat").size === 2)
    }

    "add to encoded_map with pos_map" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("10")
      val hash = feat.computeHash(10, 1000)
      val encoded_map = mutable.HashMap.empty[String, ArrayBuffer[Long]]
      val pos_map = Map((1, hash) -> 7)

      val result = feat.add(1000L, encoded_map, pos_map)
      assert(result === true)
      assert(encoded_map("test_feat") === ArrayBuffer(7L))
    }

    "throw on mismatched buffer sizes" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.raw_list.append("a")
      feat.feature_list.append(1L)
      // value_list intentionally left empty
      val builder = Example.newBuilder()
      intercept[IllegalArgumentException] {
        feat.add(1000L, builder)
      }
    }

    "getHashInfo returns detailed info" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("10,20")

      val info = feat.getHashInfo(1000)
      assert(info.size === 2)
      info.foreach { case (name, idx, fType, fmt, hash, value) =>
        assert(name === "test_feat")
        assert(idx === 1)
        assert(fType === FeatureType.Categorical)
        assert(value === 1.0F)
      }
    }

    "add to columns map" in {
      val feat = new TestCategoricalFeature(1, "test_feat")
      feat.parse("42")
      val hash = feat.computeHash(42, 1000)
      val pos_map = Map((1, hash) -> 3)
      val columns = mutable.Map.empty[String, Any]

      val result = feat.add(1000L, pos_map, columns)
      assert(result)
      assert(columns.contains("test_feat_raw"))
      assert(columns.contains("test_feat_index"))
      assert(columns.contains("test_feat_value"))
      assert(columns("test_feat_index") === Seq(0L, 3L))
    }
  }

  // ==================== ContinuousFeature ====================

  "ContinuousFeature" should {
    "getHash returns feature values directly" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("5,10,15")

      val hashes = feat.getHash(1000)
      assert(hashes === ArrayBuffer(5L, 10L, 15L))
    }

    "skip zero values in getHash" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("0,10,0,20")
      val hashes = feat.getHash(1000)
      assert(hashes === ArrayBuffer(10L, 20L))
    }

    "add to Example builder" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("5,10")
      val builder = Example.newBuilder()

      feat.add(builder)
      val example = builder.build()
      val featMap = example.getFeatures.getFeatureMap

      val indexFeat = featMap.get("conti_feat_index")
      assert(indexFeat.getInt64List.getValueList.asScala.toSeq === Seq(0L, 5L, 10L))

      val valueFeat = featMap.get("conti_feat_value")
      assert(valueFeat.getFloatList.getValueList.asScala.toSeq.map(_.toFloat) === Seq(1.0F, 5.0F, 10.0F))
    }

    "add with pos_map" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("42")
      val pos_map = Map((1, 42L) -> 7)

      val builder = Example.newBuilder()
      val result = feat.add(builder, pos_map)
      assert(result)

      val example = builder.build()
      val indexFeat = example.getFeatures.getFeatureMap.get("conti_feat_index")
      assert(indexFeat.getInt64List.getValueList.asScala.toSeq === Seq(0L, 7L))
    }

    "add to encoded_map" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("10,20")
      val encoded_map = mutable.HashMap.empty[String, ArrayBuffer[Long]]

      feat.add(encoded_map)
      assert(encoded_map("conti_feat") === ArrayBuffer(10L, 20L))
    }

    "getHashInfo returns detailed info" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("10")

      val info = feat.getHashInfo(1000)
      assert(info.size === 1)
      val (name, idx, fType, fmt, hash, value) = info.head
      assert(name === "conti_feat")
      assert(idx === 1)
      assert(fType === FeatureType.Continuous)
      assert(hash === 10L)
      assert(value === 10.0F)
    }

    "throw on mismatched buffer sizes" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.raw_list.append("a")
      // feature_list and value_list intentionally left empty
      val builder = Example.newBuilder()
      intercept[IllegalArgumentException] {
        feat.add(builder)
      }
    }

    "add to columns map" in {
      val feat = new TestContinuousFeature(1, "conti_feat")
      feat.parse("42")
      val pos_map = Map((1, 42L) -> 7)
      val columns = mutable.Map.empty[String, Any]

      val result = feat.add(pos_map, columns)
      assert(result)
      assert(columns("conti_feat_index") === Seq(0L, 7L))
    }
  }

  // ==================== CrossFeature ====================

  "CrossFeature" should {
    "enumerate all combinations with foreachCombination" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10,20")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("30")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      // CrossFeature.indexes(i) gives the current index into rnfs(i).feature_list
      val combinations = ArrayBuffer.empty[(Long, Long)]
      cross.foreachCombination {
        combinations.append((f1.feature_list(cross.indexes(0)), f2.feature_list(cross.indexes(1))))
      }
      // f1 has {10,20}, f2 has {30} => 2 combinations: (10,30), (20,30)
      assert(combinations.size === 2)
      assert(combinations.contains((10L, 30L)))
      assert(combinations.contains((20L, 30L)))
    }

    "skip combinations where any feature value is 0" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("0,10")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("20")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      val combinations = ArrayBuffer.empty[(Long, Long)]
      cross.foreachCombination {
        combinations.append((f1.feature_list(cross.indexes(0)), f2.feature_list(cross.indexes(1))))
      }
      assert(combinations.size === 1)
      assert(combinations.head === (10L, 20L))
    }

    "computeHash consistently" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("20")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      val hash1 = cross.computeHash(1000)
      val hash2 = cross.computeHash(1000)
      assert(hash1 === hash2)
      assert(hash1 >= 0 && hash1 < 1000)
    }

    "getHash returns all combination hashes" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10,20")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("30")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      val hashes = cross.getHash(1000)
      assert(hashes.size === 2)
    }

    "add to Example builder" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("20")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      val builder = Example.newBuilder()
      cross.add(1000L, builder)

      val example = builder.build()
      val featMap = example.getFeatures.getFeatureMap
      assert(featMap.containsKey("cross_f_raw"))
      assert(featMap.containsKey("cross_f_index"))
      assert(featMap.containsKey("cross_f_value"))
    }

    "add to encoded_map" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("20")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      val encoded_map = mutable.HashMap.empty[String, ArrayBuffer[Long]]
      cross.add(1000L, encoded_map)

      assert(encoded_map("cross_f").size === 1)
    }

    "getHashInfo returns detailed info" in {
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("20")

      val cross = new CrossFeature[String](100, "cross_f", f1, f2)
      val info = cross.getHashInfo(1000)
      assert(info.size === 1)
      val (name, idx, fType, fmt, _, _) = info.head
      assert(name === "cross_f")
      assert(idx === 100)
      assert(fType === FeatureType.Categorical)
      assert(fmt === "1:10__xx__2:20")
    }
  }

  // ==================== Featurizer ====================

  private class TestFeaturizer extends Featurizer[String] {
    override def setup(): Featurizer[String] = {
      target = new TestRawTarget()
      this
    }
  }

  "Featurizer" should {
    "getFieldInfo" in {
      val encoder = new TestFeaturizer()
      encoder.raw_cate_features.append(new TestCategoricalFeature(1, "cate1"))
      encoder.raw_cate_features.append(new TestCategoricalFeature(2, "cate2"))
      encoder.raw_conti_features.append(new TestContinuousFeature(3, "conti1"))
      val f1 = new TestCategoricalFeature(1, "f1")
      f1.parse("10")
      val f2 = new TestCategoricalFeature(2, "f2")
      f2.parse("20")
      encoder.cross_features.append(new CrossFeature[String](100, "cross1", f1, f2))

      val fields = encoder.getFieldInfo()
      assert(fields === ArrayBuffer(
        ("cate1", 1), ("cate2", 2), ("conti1", 3), ("cross1", 100)
      ))
    }

    "get_parquet_column_names" in {
      val encoder = new TestFeaturizer()
      encoder.raw_cate_features.append(new TestCategoricalFeature(1, "cate1"))

      val names = encoder.get_parquet_column_names()
      assert(names === ArrayBuffer("target", "cate1"))
    }

    "encode input to Example builder" in {
      val encoder = new TestFeaturizer()
      encoder.setup()
      encoder.raw_cate_features.append(new TestCategoricalFeature(1, "cate1"))
      encoder.raw_cate_features.append(new TestCategoricalFeature(2, "cate2"))

      val builder = Example.newBuilder()
      encoder.encode("42,100", 1000L, builder)
      val example = builder.build()

      val featMap = example.getFeatures.getFeatureMap
      assert(featMap.containsKey("target"))   // target parsed from "42,100".head -> 42.0F
      assert(featMap.containsKey("cate1_raw"))
      assert(featMap.containsKey("cate2_raw"))
    }

    "encode with pos_map and target_map" in {
      val encoder = new TestFeaturizer()
      encoder.setup()
      val feat = new TestCategoricalFeature(1, "cate1")
      encoder.raw_cate_features.append(feat)

      feat.parse("42")
      val hash = feat.computeHash(42, 1000)
      val pos_map = Map((1, hash) -> 3)
      val target_map = Map(42 -> 1)

      val builder = Example.newBuilder()
      val (hasFeature, hasTarget) = encoder.encode("42", 1000L, builder, pos_map, target_map)
      assert(hasFeature)
      assert(hasTarget)
    }

    "encode to TSV format" in {
      val encoder = new TestFeaturizer()
      encoder.setup()
      encoder.raw_cate_features.append(new TestCategoricalFeature(1, "cate1"))

      val result = encoder.encode("10", 1000L, ",", ":")
      assert(result.nonEmpty)
      // format: key:value,key:value,...
      assert(result.contains("cate1:"))
    }
  }
}
