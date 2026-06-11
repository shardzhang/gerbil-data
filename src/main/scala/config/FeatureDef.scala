package config

/** Configuration for a single feature extracted from YAML. */
case class FeatureDef(
  /** Human-readable feature name (used as prefix for TFRecord fields: {name}_raw, {name}_index, {name}_value). */
  name: String,
  /** Unique numeric feature index within the feature set. */
  index: Int,
  /** "categorical" or "continuous". */
  `type`: String,
  /** ML1MSample field name accessed via reflection (for generic features). */
  source: Option[String] = None,
  /** Fully-qualified Scala class name (for custom features with complex parse() logic). */
  className: Option[String] = None,
  /** Whether this feature is active. */
  enabled: Option[Boolean] = None,
  /** Value-to-bucket mapping (exact match). */
  mapping: Option[Map[String, Int]] = None,
  /** Default bucket ID when mapping has no match. */
  default: Option[Int] = None,
  /** Bucket split boundaries: value <= boundaries[0] → 1, <= boundaries[1] → 2, ... */
  boundaries: Option[Seq[Double]] = None,
  /** Constant added to source value before using as bucket ID. */
  offset: Option[Int] = None
) {
  def isEnabled: Boolean = enabled.getOrElse(true)
  def isCategorical: Boolean = `type` == "categorical"
  def isContinuous: Boolean = `type` == "continuous"
  def isCustom: Boolean = className.isDefined
}

/** Configuration for a cross feature (pair/combination of two categorical features). */
case class CrossFeatureDef(
  name: String,
  index: Int,
  left: String,
  right: String,
  left2: Option[String] = None
)

/** Top-level feature configuration loaded from YAML. */
case class FeatureConfig(
  hash_dim: Long,
  features: Seq[FeatureDef],
  cross_features: Option[CrossConfig] = None
)

case class CrossConfig(
  enabled: Option[Boolean] = None,
  pairs: Option[Seq[CrossFeatureDef]] = None
) {
  def isEnabled: Boolean = enabled.getOrElse(false)
}
