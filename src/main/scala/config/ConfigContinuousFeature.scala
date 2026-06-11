package config

import featurizer.core.{ContinuousFeature, RawFeature}
import featurizer.ml1m.ML1MSample

/** A generic continuous feature driven by FeatureDef configuration.
  * Reads the source field value and uses it directly as the numeric value.
  */
class ConfigContinuousFeature(defn: FeatureDef)
  extends ContinuousFeature[ML1MSample](defn.index, defn.name) {

  private val sourceField = defn.source.getOrElse(
    throw new IllegalArgumentException("ConfigContinuousFeature needs 'source': " + defn.name)
  )
  private val getter = classOf[ML1MSample].getMethod(sourceField)

  override def parse(sample: ML1MSample): RawFeature = {
    val rawValue = getter.invoke(sample)
    val rawStr = if (rawValue == null) "0.0" else rawValue.toString
    val numVal = try {
      rawStr.toFloat
    } catch {
      case _: NumberFormatException => 0.0f
    }
    raw_list.append(rawStr)
    feature_list.append(1L)
    value_list.append(numVal)
    this
  }
}
