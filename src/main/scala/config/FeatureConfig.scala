package config

/** Configuration for a single feature extracted from YAML. */
case class FeatureDef(
  /** Human-readable feature name (used as prefix for TFRecord fields). */
  name: String,
  /** Unique numeric feature index within the feature set. */
  index: Int,
  /** Feature type: 1 = categorical (离散), 0 = continuous (连续). */
  `type`: Int,
  /** Fully-qualified Scala class name that implements parse() logic. */
  className: String,
  /** Whether this feature is active. */
  enabled: Option[Boolean] = None
) {
  def isEnabled: Boolean = enabled.getOrElse(true)
  def isCategorical: Boolean = `type` == 1
  def isContinuous: Boolean = `type` == 0
}

/** Top-level feature configuration loaded from YAML. */
case class FeatureConfig(
  /** Default package prefix for className resolution. */
  pkg: Option[String] = None,
  /** List of feature definitions. */
  features: Seq[FeatureDef],
  /** Cross feature definitions (empty = disabled). */
  cross_features: Option[Seq[CrossFeatureDef]] = None
)

/** Configuration for a cross feature (combination of two or more categorical features). */
case class CrossFeatureDef(
  /** Human-readable cross feature name. */
  name: String,
  /** Unique numeric feature index. */
  index: Int,
  /** Whether this cross feature is active. */
  enabled: Option[Boolean] = None,
  /** Names of the features to cross (2 or 3). */
  depends: Seq[String]
) {
  def isEnabled: Boolean = enabled.getOrElse(true)
}
