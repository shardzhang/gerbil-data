package config

import java.io.{FileInputStream, InputStream}
import java.util
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Loads FeatureConfig from a YAML file. */
object FeatureConfigLoader {

  /** Loads config from a classpath resource (e.g. "/ml1m/features.yaml"). */
  def loadFromResource(resourcePath: String = "/features.yaml"): FeatureConfig = {
    val is = getClass.getResourceAsStream(resourcePath)
    if (is == null) {
      throw new RuntimeException("Feature config not found on classpath: " + resourcePath)
    }
    try {
      parseYaml(is)
    } finally {
      is.close()
    }
  }

  /** Loads config from an external file path (for development / debugging). */
  def loadFromFile(filePath: String): FeatureConfig = {
    val is = new FileInputStream(filePath)
    try {
      parseYaml(is)
    } finally {
      is.close()
    }
  }

  /** Parses YAML input stream into a nested Java Map, then converts to FeatureConfig. */
  private def parseYaml(is: InputStream): FeatureConfig = {
    val yaml = new Yaml()
    val raw = yaml.load(is).asInstanceOf[util.Map[String, Any]]
    convert(raw)
  }

  /** Converts the top-level YAML map into a FeatureConfig case class. */
  private def convert(raw: util.Map[String, Any]): FeatureConfig = {
    val pkg = Option(raw.get("pkg")).map(_.toString)
    val features = ArrayBuffer.empty[FeatureDef]
    val rawFeatures = raw.getOrDefault("features", new util.ArrayList[Any]())
      .asInstanceOf[util.List[util.Map[String, Any]]]
    for (rf <- rawFeatures.asScala) {
      features += convertFeature(rf, pkg)
    }

    val crossRaw = raw.getOrDefault("cross_features", null)
    val crossConfig = if (crossRaw != null) {
      val rawList = crossRaw.asInstanceOf[util.List[util.Map[String, Any]]]
      Some(rawList.asScala.map(m => convertCrossFeature(m)).toSeq)
    } else {
      None
    }

    FeatureConfig(pkg, features, crossConfig)
  }

  /** Converts a single YAML feature map into a FeatureDef. Resolves className via pkg prefix. */
  private def convertFeature(raw: util.Map[String, Any], pkg: Option[String]): FeatureDef = {
    val name = raw.get("name").toString
    val index = raw.get("index").toString.toInt
    val rawClass = raw.get("class").toString
    val className = if (pkg.isDefined && !rawClass.contains(".")) {
      pkg.get + "." + rawClass
    } else {
      rawClass
    }
    val enabled = Option(raw.get("enabled")).map(v => java.lang.Boolean.parseBoolean(v.toString))
    val rawType = raw.get("type").toString.toInt
    FeatureDef(name, index, rawType, className, enabled)
  }

  /** Converts a single cross feature map into CrossFeatureDef. */
  private def convertCrossFeature(raw: util.Map[String, Any]): CrossFeatureDef = {
    val name = raw.get("name").toString
    val index = raw.get("index").toString.toInt
    val enabled = Option(raw.get("enabled")).map(v => java.lang.Boolean.parseBoolean(v.toString))
    val dependsRaw = raw.get("depends").asInstanceOf[util.List[Any]]
    val depends = dependsRaw.asScala.map(_.toString).toSeq
    CrossFeatureDef(name, index, enabled, depends)
  }
}
