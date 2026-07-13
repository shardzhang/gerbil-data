package featurizer.alictr

import config.{FeatureConfig, FeatureConfigLoader, FeatureDef}
import featurizer.{CategoricalFeature, ContinuousFeature, CrossFeature, Featurizer}
import utils.LogUtils.green_println

import scala.collection.mutable

class AliCtrFeaturizer(configPath: Option[String] = None, targetMode: String = "binary")
  extends Featurizer[AliCtrSample] {

  private lazy val config: FeatureConfig = configPath match {
    case Some(path) => FeatureConfigLoader.loadFromFile(path)
    case None => FeatureConfigLoader.loadFromResource("/alictr/alictr_features.yaml")
  }

  private def instantiate(featureDef: FeatureDef): Any = {
    try {
      val clazz = Class.forName(featureDef.class_name)
      val ctor = clazz.getConstructor(classOf[Int], classOf[String])
      ctor.newInstance(featureDef.field_index.asInstanceOf[AnyRef], featureDef.field_name)
    } catch {
      case e: RuntimeException => throw e
      case e: Exception =>
        throw new RuntimeException("[AliCtrFeaturizer] failed to instantiate feature '" + featureDef.field_name +
          "' (index=" + featureDef.field_index + "): " + featureDef.class_name, e)
    }
  }

  override def setup(): Featurizer[AliCtrSample] = {
    raw_cate_features.clear()
    raw_conti_features.clear()
    cross_features.clear()
    target = targetMode match {
      case "binary" => new AliCtrLabel()
      case "multi"  => new AliCtrTarget()
      case "rating" => new AliCtrRating()
      case _        => throw new IllegalArgumentException(s"Unknown target_mode: '$targetMode'")
    }

    val featureDefs: Seq[FeatureDef] = config.features.filter(_.isEnabled)
    val featureInstances: mutable.Map[String, Any] = scala.collection.mutable.Map.empty
    for (defn <- featureDefs) {
      val inst = instantiate(defn)
      if (defn.isCategorical) {
        raw_cate_features.append(inst.asInstanceOf[CategoricalFeature[AliCtrSample]])
      } else {
        raw_conti_features.append(inst.asInstanceOf[ContinuousFeature[AliCtrSample]])
      }
      featureInstances(defn.field_name) = inst
    }

    if (config.cross_features.isDefined) {
      for (cfd <- config.cross_features.get) {
        if (cfd.isEnabled) {
          val feats = cfd.depends.flatMap { name =>
            featureInstances.get(name).map(_.asInstanceOf[CategoricalFeature[AliCtrSample]])
          }
          if (feats.size < cfd.depends.size) {
            val missing = cfd.depends.filterNot(featureInstances.contains).mkString(", ")
            green_println("[AliCtrFeaturizer] WARN: cross feature " + cfd.field_name + " depends not found: " + missing)
          } else {
            cross_features.append(new CrossFeature(cfd.field_index, cfd.field_name, feats: _*))
          }
        }
      }
    }
    this
  }
}
