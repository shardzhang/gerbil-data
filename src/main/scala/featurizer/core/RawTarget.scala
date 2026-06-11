package featurizer.core

import org.tensorflow.example.Example
import tfrecords.serde.FloatListFeatureEncoder

import scala.collection.mutable

/**
 * @author shard zhang
 * @date 2026/6/10 17:57
 * @note Abstract prediction target base — parses labels, encodes into TFRecord/Parquet
 */

/** Base class for prediction targets. Parses a raw sample to extract the label and encodes it into TFRecord or Parquet with optional target-map lookup. */
abstract class RawTarget[T] extends Serializable {
  /** The parsed target value (float). */
  var target: Float = 0.0F

  /** Extracts the target value from the input sample. */
  def parse(input: T): RawTarget[T]

  /** Adds the target to a TF Example builder (raw value, no map lookup). */
  def add(builder: Example.Builder): Unit = {
    builder.getFeaturesBuilder
      .putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
  }

  /** Adds the target to a TF Example builder with target-map lookup. Returns false if target is not in map. */
  def add(builder: Example.Builder, target_map: collection.Map[Int, Int]): Boolean = {
    if (target_map == null) {
      builder.getFeaturesBuilder
        .putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
      return true
    }
    if (target_map.contains(target.toInt)) {
      builder.getFeaturesBuilder.putFeature(
        "target", FloatListFeatureEncoder.encode(Seq(target_map(target.toInt).toFloat))
      )
      return true
    }
    false
  }

  /** Adds the target to a Parquet columns map with target-map lookup. Returns false if target is not in map. */
  def add(map: mutable.Map[String, Any], target_map: collection.Map[Int, Int]): Boolean = {
    if (target_map == null) {
      map.put("target", target)
      return true
    }
    if (target_map.contains(target.toInt)) {
      map.put("target", target_map(target.toInt).toFloat)
      return true
    }
    false
  }
}
