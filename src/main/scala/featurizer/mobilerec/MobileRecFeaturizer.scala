package featurizer.mobilerec

import config.{FeatureConfig, FeatureConfigLoader, FeatureDef}
import featurizer.{CategoricalFeature, ContinuousFeature, CrossFeature, Featurizer, RawTarget}
import utils.LogUtils.green_println

import scala.collection.mutable

class MobileRecFeaturizer(configPath: Option[String] = None, targetMode: String = "binary")
  extends Featurizer[MobileRecSample] {

  private lazy val config: FeatureConfig = configPath match {
    case Some(path) => FeatureConfigLoader.loadFromFile(path)
    case None => FeatureConfigLoader.loadFromResource("/mobilerec/mobilerec_features.yaml")
  }

  private def instantiate(featureDef: FeatureDef): Any = {
    try {
      val clazz = Class.forName(featureDef.class_name)
      val ctor = clazz.getConstructor(classOf[Int], classOf[String])
      ctor.newInstance(featureDef.field_index.asInstanceOf[AnyRef], featureDef.field_name)
    } catch {
      case e: RuntimeException => throw e
      case e: Exception =>
        throw new RuntimeException("[MobileRecFeaturizer] failed to instantiate feature '" + featureDef.field_name +
          "' (index=" + featureDef.field_index + "): " + featureDef.class_name, e)
    }
  }

  override def setup(): Featurizer[MobileRecSample] = {
    raw_cate_features.clear()
    raw_conti_features.clear()
    cross_features.clear()
    target = targetMode match {
      case "binary" => new MobileRecLabel()
      case "multi"  => new MobileRecTarget()
      case "rating" => new MobileRecRating()
      case _        => throw new IllegalArgumentException(s"Unknown target_mode: '$targetMode'. Expected 'binary', 'multi', or 'rating'")
    }

    val featureDefs: Seq[FeatureDef] = config.features.filter(_.isEnabled)
    val featureInstances: mutable.Map[String, Any] = scala.collection.mutable.Map.empty
    for (defn <- featureDefs) {
      val inst = instantiate(defn)
      if (defn.isCategorical) {
        raw_cate_features.append(inst.asInstanceOf[CategoricalFeature[MobileRecSample]])
      } else {
        raw_conti_features.append(inst.asInstanceOf[ContinuousFeature[MobileRecSample]])
      }
      featureInstances(defn.field_name) = inst
    }

    if (config.cross_features.isDefined) {
      for (cfd <- config.cross_features.get) {
        if (cfd.isEnabled) {
          val feats = cfd.depends.flatMap { name =>
            featureInstances.get(name).map(_.asInstanceOf[CategoricalFeature[MobileRecSample]])
          }
          if (feats.size < cfd.depends.size) {
            val missing = cfd.depends.filterNot(featureInstances.contains).mkString(", ")
            green_println("[MobileRecFeaturizer] WARN: cross feature " + cfd.field_name + " depends not found: " + missing)
          } else {
            cross_features.append(new CrossFeature(cfd.field_index, cfd.field_name, feats: _*))
          }
        }
      }
    }
    this
  }
}
