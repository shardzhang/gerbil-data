package featurizer.ml1m

import featurizer.RawTarget
import org.tensorflow.example.Example
import tfrecords.serde.FloatListFeatureEncoder

import scala.collection.mutable


/**
 * Prediction target extractor for supervised learning.
 *
 * Parses a raw sample to extract the label and encodes it into TFRecord or Parquet.
 * Supports optional target-map vocabulary lookup for mapping raw labels to contiguous indices
 * (e.g. mapping sparse class IDs to dense indices for multi-class classification).
 *
 * @tparam T the raw sample type
 */
class ML1MTarget extends RawTarget[ML1MSample] {
  /** The parsed target value (float). */
  var target: Float = 0.0F
  var label: Float = 0.0F
  var rating: Float = 0.0F

  /**
   * ML-1M target. Extracts the target value from the input sample
   * - binary-class(ctr predict) target: task using 0/1 as label
   * - Multi-class target: using item_id as class ID
   * - Regression target: using rating as label
   */
  override def parse(sample: ML1MSample) = {
    target = sample.target // item_id
    label = sample.label
    rating = sample.rating
    this
  }

  /** Adds the target to a TF Example builder (raw value, no vocabulary lookup). */
  override def add(builder: Example.Builder): Unit = {
    builder.getFeaturesBuilder.putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
    builder.getFeaturesBuilder.putFeature("label", FloatListFeatureEncoder.encode(Seq(label)))
    builder.getFeaturesBuilder.putFeature("rating", FloatListFeatureEncoder.encode(Seq(rating)))
  }

  /** Adds the target to a TF Example builder with target-map vocabulary lookup. Returns false if target is not in map. */
  override def add(builder: Example.Builder, target_map: collection.Map[Int, Int]): Boolean = {
    if (target_map == null || target_map.isEmpty) {
      builder.getFeaturesBuilder.putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
      builder.getFeaturesBuilder.putFeature("label", FloatListFeatureEncoder.encode(Seq(label)))
      builder.getFeaturesBuilder.putFeature("rating", FloatListFeatureEncoder.encode(Seq(rating)))
      return true
    }
    if (target_map.contains(target.toInt)) {
      builder.getFeaturesBuilder.putFeature("target", FloatListFeatureEncoder.encode(Seq(target_map(target.toInt).toFloat)))
      builder.getFeaturesBuilder.putFeature("label", FloatListFeatureEncoder.encode(Seq(label)))
      builder.getFeaturesBuilder.putFeature("rating", FloatListFeatureEncoder.encode(Seq(rating)))
      return true
    }
    false
  }

  /** Adds the target to a Parquet columns map with target-map vocabulary lookup. Returns false if target is not in map. */
  override def add(map: mutable.Map[String, Any], target_map: collection.Map[Int, Int]): Boolean = {
    if (target_map == null) {
      map.put("target", target)
      map.put("label", label)
      map.put("rating", rating)
      return true
    }
    if (target_map.contains(target.toInt)) {
      map.put("target", target_map(target.toInt).toFloat)
      map.put("label", label)
      map.put("rating", rating)
      return true
    }
    false
  }
}
