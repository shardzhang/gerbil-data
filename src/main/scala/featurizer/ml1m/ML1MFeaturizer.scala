package featurizer.ml1m

import featurizer.core.{CategoricalFeature, ContinuousFeature, CrossFeature, Featurizer, RawTarget}
import config.{FeatureConfig, FeatureConfigLoader, FeatureDef}
import utils.LogUtils.green_println
import scala.collection.mutable

/**
 * ML-1M featurizer orchestrator — reads features.yaml, instantiates and registers all feature extractors
 */

/** Featurizes each ML1M sample before feeding to the model.
 * Reads feature registry from features.yaml; all parse() logic stays in Scala classes.
 */
class ML1MFeaturizer(configPath: Option[String] = None) extends Featurizer[ML1MSample] {

  private lazy val config: FeatureConfig = configPath match {
    case Some(path) => FeatureConfigLoader.loadFromFile(path)
    case None => FeatureConfigLoader.loadFromResource("/ml1m/features.yaml")
  }

  private def instantiate(featureDef: FeatureDef): Any = {
    try {
      val clazz = Class.forName(featureDef.className)
      val ctor = clazz.getConstructor(classOf[Int], classOf[String])
      ctor.newInstance(featureDef.index.asInstanceOf[AnyRef], featureDef.name)
    } catch {
      case e: RuntimeException => throw e
      case e: Exception =>
        throw new RuntimeException("[Featurizer] failed to instantiate feature '" + featureDef.name +
          "' (index=" + featureDef.index + "): " + featureDef.className, e)
    }
  }

  override def setup(): Featurizer[ML1MSample] = {
    raw_cate_features.clear()
    raw_conti_features.clear()
    cross_features.clear()
    target = new Target()

    val featureDefs: Seq[FeatureDef] = config.features.filter(_.isEnabled)
    val featureInstances: mutable.Map[String, Any] = scala.collection.mutable.Map.empty
    for (defn <- featureDefs) {
      val inst = instantiate(defn)
      if (defn.isCategorical) {
        raw_cate_features.append(inst.asInstanceOf[CategoricalFeature[ML1MSample]])
      } else {
        raw_conti_features.append(inst.asInstanceOf[ContinuousFeature[ML1MSample]])
      }
      featureInstances(defn.name) = inst
    }

    if (config.cross_features.isDefined) {
      for (cfd <- config.cross_features.get) {
        if (cfd.isEnabled) {
          val feats = cfd.depends.flatMap { name =>
            featureInstances.get(name).map(_.asInstanceOf[CategoricalFeature[ML1MSample]])
          }
          if (feats.size < cfd.depends.size) {
            val missing = cfd.depends.filterNot(featureInstances.contains).mkString(", ")
            green_println("[Featurizer] WARN: cross feature " + cfd.name + " depends not found: " + missing)
          } else {
            cross_features.append(new CrossFeature(cfd.index, cfd.name, feats: _*))
          }
        }
      }
    }

    this
  }
}
