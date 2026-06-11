package config

import java.io.{FileInputStream, InputStream}
import java.util
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Loads FeatureConfig from a YAML file. */
object FeatureConfigLoader {

  /** Loads from the classpath resource (default: /features.yaml). */
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

  /** Loads from an external file path. */
  def loadFromFile(filePath: String): FeatureConfig = {
    val is = new FileInputStream(filePath)
    try {
      parseYaml(is)
    } finally {
      is.close()
    }
  }

  private def parseYaml(is: InputStream): FeatureConfig = {
    val yaml = new Yaml()
    val raw = yaml.load(is).asInstanceOf[util.Map[String, Any]]
    convert(raw)
  }

  private def convert(raw: util.Map[String, Any]): FeatureConfig = {
    val hashDim = raw.getOrDefault("hash_dim", "1152921504606846976").toString.toLong

    val features = ArrayBuffer.empty[FeatureDef]
    val rawFeatures = raw.getOrDefault("features", new util.ArrayList[Any]())
      .asInstanceOf[util.List[util.Map[String, Any]]]
    for (rf <- rawFeatures.asScala) {
      features += convertFeature(rf)
    }

    val crossRaw = raw.getOrDefault("cross_features", null)
    val crossConfig = if (crossRaw != null) {
      Some(convertCrossConfig(crossRaw.asInstanceOf[util.Map[String, Any]]))
    } else {
      None
    }

    FeatureConfig(hashDim, features.toSeq, crossConfig)
  }

  private def convertFeature(raw: util.Map[String, Any]): FeatureDef = {
    val name = raw.get("name").toString
    val index = raw.get("index").toString.toInt
    val fType = raw.get("type").toString

    val source = Option(raw.get("source")).map(_.toString)
    val className = Option(raw.get("class")).map(_.toString)
    // "class" in YAML maps to "className" in Scala (class is a reserved keyword)
    val enabled = Option(raw.get("enabled")).map(v => java.lang.Boolean.parseBoolean(v.toString))

    val mappingRaw = Option(raw.get("mapping")).map(_.asInstanceOf[util.Map[Any, Any]])
    val mapping = mappingRaw.map { m =>
      m.asScala.map { case (k, v) => k.toString -> v.toString.toInt }.toMap
    }
    val default = Option(raw.get("default")).map(_.toString.toInt)

    val boundariesRaw = Option(raw.get("boundaries")).map(_.asInstanceOf[util.List[Any]])
    val boundaries = boundariesRaw.map { b =>
      b.asScala.map(_.toString.toDouble).toSeq
    }
    val offset = Option(raw.get("offset")).map(_.toString.toInt)

    FeatureDef(name, index, fType, source, className, enabled, mapping, default, boundaries, offset)
  }

  private def convertCrossConfig(raw: util.Map[String, Any]): CrossConfig = {
    val enabled = Option(raw.get("enabled")).map(v => java.lang.Boolean.parseBoolean(v.toString))
    val pairsRaw = Option(raw.get("pairs")).map(_.asInstanceOf[util.List[util.Map[String, Any]]])
    val pairs = pairsRaw.map { list =>
      list.asScala.map { m =>
        val name = m.get("name").toString
        val index = m.get("index").toString.toInt
        val left = m.get("left").toString
        val right = m.get("right").toString
        val left2 = Option(m.get("left2")).map(_.toString)
        CrossFeatureDef(name, index, left, right, left2)
      }.toSeq
    }
    CrossConfig(enabled, pairs)
  }
}
