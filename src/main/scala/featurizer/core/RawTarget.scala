package featurizer.core

import org.tensorflow.example.Example
import tfrecords.serde.FloatListFeatureEncoder

import scala.collection.mutable

abstract class RawTarget[T] extends Serializable {
  var target: Float = 0.0F

  def parse(input: T): RawTarget[T]

  def add(builder: Example.Builder): Unit = {
    builder.getFeaturesBuilder
      .putFeature("target", FloatListFeatureEncoder.encode(Seq(target)))
  }

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
