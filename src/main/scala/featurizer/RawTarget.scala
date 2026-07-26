package featurizer

import org.tensorflow.example.Example
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
abstract class RawTarget[T] extends Serializable {
  /** Extracts the target value from the input sample. */
  def parse(input: T): RawTarget[T]

  /** Adds the target to a TF Example builder (raw value, no vocabulary lookup). */
  def add(builder: Example.Builder): Unit

  /** Adds the target to a TF Example builder with target-map vocabulary lookup. Returns false if target is not in map. */
  def add(builder: Example.Builder, target_map: collection.Map[Int, Int]): Boolean

  /** Adds the target to a Parquet columns map with target-map vocabulary lookup. Returns false if target is not in map. */
  def add(map: mutable.Map[String, Any], target_map: collection.Map[Int, Int]): Boolean
}
