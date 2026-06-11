package config

import featurizer.core.{CategoricalFeature, RawFeature}
import featurizer.ml1m.ML1MSample

/** A generic categorical feature driven by FeatureDef configuration.
  * Supports three extraction modes:
  *   1. Direct: field value → bucket ID (via toLong)
  *   2. Mapping: field value → lookup in mapping map → bucket ID
  *   3. Boundaries: numeric field → bucket by ≤ boundary
  *   4. Offset: field value + offset → bucket ID
  */
class ConfigCategoricalFeature(defn: FeatureDef)
  extends CategoricalFeature[ML1MSample](defn.index, defn.name) {

  private val sourceField = defn.source.getOrElse(
    throw new IllegalArgumentException("ConfigCategoricalFeature needs 'source': " + defn.name)
  )
  private val getter = classOf[ML1MSample].getMethod(sourceField)

  override def parse(sample: ML1MSample): RawFeature = {
    val rawValue = getter.invoke(sample)
    val rawStr = if (rawValue == null) "" else rawValue.toString

    val buck = computeBucket(rawValue, rawStr)

    raw_list.append(rawStr)
    feature_list.append(buck)
    value_list.append(1.0f)
    this
  }

  private def computeBucket(rawValue: Any, rawStr: String): Long = {
    // 1. Mapping mode
    if (defn.mapping.isDefined) {
      return defn.mapping.get.getOrElse(rawStr, defn.default.getOrElse(0)).toLong
    }

    // 2. Offset mode
    if (defn.offset.isDefined) {
      val base = try {
        rawStr.toLong
      } catch {
        case _: NumberFormatException => 0L
      }
      return base + defn.offset.get
    }

    // 3. Boundaries mode
    if (defn.boundaries.isDefined) {
      val numVal = try {
        rawStr.toDouble
      } catch {
        case _: NumberFormatException => return 0L
      }
      val bounds = defn.boundaries.get
      var b = 1
      var found = false
      for (i <- bounds.indices) {
        if (!found && numVal <= bounds(i)) {
          b = i + 1
          found = true
        }
      }
      if (!found) {
        b = bounds.size + 1
      }
      return b.toLong
    }

    // 4. Direct mode
    try {
      rawStr.toLong
    } catch {
      case _: NumberFormatException =>
        try {
          rawStr.toDouble.toLong
        } catch {
          case _: NumberFormatException => 0L
        }
    }
  }
}
