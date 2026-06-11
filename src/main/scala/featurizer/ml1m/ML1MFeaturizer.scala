package featurizer.ml1m

import featurizer.ml1m.ML1MSample
import featurizer.core.{CrossFeature, Featurizer, RawTarget}
import config.{ConfigCategoricalFeature, ConfigContinuousFeature, FeatureConfig, FeatureConfigLoader, FeatureDef}
import utils.LogUtils.green_println

/** Featurizes each ML1M sample before feeding to the model.
  * Reads feature definitions from features.yaml instead of hardcoding.
  */
class ML1MFeaturizer extends Featurizer[ML1MSample] {

  /** Feature configuration loaded from YAML. Lazy to avoid initialization order issues. */
  private lazy val config: FeatureConfig = FeatureConfigLoader.loadFromResource()

  /** Name-to-feature-def lookup for cross feature resolution. */
  private var nameToDef: Map[String, FeatureDef] = Map.empty

  override def setup(): Featurizer[ML1MSample] = {
    raw_cate_features.clear()
    raw_conti_features.clear()
    cross_features.clear()

    target = new Target()

    val featureDefs = config.features.filter(_.isEnabled)
    nameToDef = featureDefs.map(f => f.name -> f).toMap

    // Build a name-to-instance map for cross feature resolution
    val featureInstances = scala.collection.mutable.Map.empty[String, Any]

    for (defn <- featureDefs) {
      if (defn.isCustom) {
        // Instantiate custom feature class via reflection
        val instance = instantiateCustom(defn)
        if (defn.isCategorical) {
          raw_cate_features.append(instance.asInstanceOf[featurizer.core.CategoricalFeature[ML1MSample]])
        } else {
          raw_conti_features.append(instance.asInstanceOf[featurizer.core.ContinuousFeature[ML1MSample]])
        }
        featureInstances(defn.name) = instance
        green_println("[Featurizer] Registered custom feature: " + defn.name + " (index=" + defn.index + ")")
      } else if (defn.isCategorical) {
        val inst = new ConfigCategoricalFeature(defn)
        raw_cate_features.append(inst)
        featureInstances(defn.name) = inst
        green_println("[Featurizer] Registered config feature: " + defn.name + " (index=" + defn.index + ")")
      } else if (defn.isContinuous) {
        val inst = new ConfigContinuousFeature(defn)
        raw_conti_features.append(inst)
        featureInstances(defn.name) = inst
        green_println("[Featurizer] Registered config feature: " + defn.name + " (index=" + defn.index + ")")
      }
    }

    // Build cross features if enabled
    if (config.cross_features.isDefined && config.cross_features.get.isEnabled) {
      val crossPairs = config.cross_features.get.pairs.getOrElse(Seq.empty)
      for (cp <- crossPairs) {
        val leftFeat = featureInstances.get(cp.left)
        val rightFeat = featureInstances.get(cp.right)
        if (leftFeat.isEmpty) {
          green_println("[Featurizer] WARN: cross feature " + cp.name + " left feature '" + cp.left + "' not found")
        }
        if (rightFeat.isEmpty) {
          green_println("[Featurizer] WARN: cross feature " + cp.name + " right feature '" + cp.right + "' not found")
        }
        if (leftFeat.isDefined && rightFeat.isDefined) {
          val left = leftFeat.get.asInstanceOf[featurizer.core.CategoricalFeature[ML1MSample]]
          val right = rightFeat.get.asInstanceOf[featurizer.core.CategoricalFeature[ML1MSample]]
          val cross = if (cp.left2.isDefined) {
            val left2Feat = featureInstances.get(cp.left2.get)
            if (left2Feat.isDefined) {
              new CrossFeature(cp.index, cp.name, left, left2Feat.get.asInstanceOf[featurizer.core.CategoricalFeature[ML1MSample]], right)
            } else {
              green_println("[Featurizer] WARN: cross feature " + cp.name + " left2 feature '" + cp.left2.get + "' not found, skipping")
              null
            }
          } else {
            new CrossFeature(cp.index, cp.name, left, right)
          }
          if (cross != null) {
            cross_features.append(cross)
            green_println("[Featurizer] Registered cross feature: " + cp.name + " (index=" + cp.index + ")")
          }
        }
      }
    } else {
      green_println("[Featurizer] Cross features disabled in config")
    }

    green_println("[Featurizer] Setup complete: " +
      raw_cate_features.size + " categorical, " +
      raw_conti_features.size + " continuous, " +
      cross_features.size + " cross features")

    this
  }

  /** Instantiates a custom feature class by reflection.
    * Looks for constructor (Int, String) matching existing feature classes.
    */
  private def instantiateCustom(defn: FeatureDef): Any = {
    val clazz = Class.forName(defn.className.get)
    val ctor = clazz.getConstructor(classOf[Int], classOf[String])
    ctor.newInstance(defn.index.asInstanceOf[AnyRef], defn.name)
  }
}
