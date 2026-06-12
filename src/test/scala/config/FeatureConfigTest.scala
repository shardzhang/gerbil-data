package config

import org.scalatest.{Matchers, WordSpec}

class FeatureConfigTest extends WordSpec with Matchers {

  "FeatureDef" should {
    "create with all fields" in {
      val f = FeatureDef("test_feat", 10, 1, "com.example.TestFeature", Some(true))
      assert(f.name === "test_feat")
      assert(f.index === 10)
      assert(f.`type` === 1)
      assert(f.className === "com.example.TestFeature")
      assert(f.isEnabled === true)
      assert(f.isCategorical === true)
      assert(f.isContinuous === false)
    }

    "default enabled to true when None" in {
      val f = FeatureDef("feat", 1, 0, "com.example.Feat")
      assert(f.isEnabled === true)
    }

    "respect enabled=false" in {
      val f = FeatureDef("feat", 1, 0, "com.example.Feat", Some(false))
      assert(f.isEnabled === false)
    }

    "detect categorical vs continuous" in {
      val cat = FeatureDef("cat", 1, 1, "com.example.Cat")
      val cont = FeatureDef("cont", 2, 0, "com.example.Cont")
      assert(cat.isCategorical && !cat.isContinuous)
      assert(cont.isContinuous && !cont.isCategorical)
    }
  }

  "CrossFeatureDef" should {
    "create and check enabled" in {
      val c = CrossFeatureDef("cross", 100, Some(true), Seq("a", "b"))
      assert(c.name === "cross")
      assert(c.index === 100)
      assert(c.isEnabled === true)
      assert(c.depends === Seq("a", "b"))
    }

    "default enabled to true" in {
      val c = CrossFeatureDef("cross", 100, depends = Seq("a"))
      assert(c.isEnabled === true)
    }
  }

  "FeatureConfig" should {
    "create with all fields" in {
      val features = Seq(
        FeatureDef("f1", 1, 1, "pkg.F1"),
        FeatureDef("f2", 2, 0, "pkg.F2")
      )
      val crosses = Some(Seq(
        CrossFeatureDef("c1", 100, depends = Seq("f1", "f2"))
      ))
      val config = FeatureConfig(Some("test.pkg"), features, crosses)
      assert(config.pkg === Some("test.pkg"))
      assert(config.features.size === 2)
      assert(config.cross_features.get.size === 1)
    }

    "handle missing cross features" in {
      val config = FeatureConfig(features = Seq.empty, cross_features = None)
      assert(config.cross_features === None)
    }
  }

  "FeatureConfigLoader" should {
    "load from classpath resource" in {
      val config = FeatureConfigLoader.loadFromResource("/config/test_features.yaml")
      assert(config.pkg === Some("featurizer.ml1m"))
      val feats = config.features
      assert(feats.size === 4)
      assert(feats.exists(_.name === "user_age"))
      assert(feats.exists(_.name === "user_gender"))
      assert(feats.exists(_.name === "movie_avg_rate_continue"))
      assert(feats.exists(_.name === "disabled_feat"))
    }

    "resolve className via pkg prefix" in {
      val config = FeatureConfigLoader.loadFromResource("/config/test_features.yaml")
      val userAge = config.features.find(_.name === "user_age").get
      assert(userAge.className === "featurizer.ml1m.UserAge")
    }

    "parse categorical and continuous types" in {
      val config = FeatureConfigLoader.loadFromResource("/config/test_features.yaml")
      val catFeat = config.features.find(_.name === "user_age").get
      val contFeat = config.features.find(_.name === "movie_avg_rate_continue").get
      assert(catFeat.isCategorical === true)
      assert(contFeat.isContinuous === true)
    }

    "parse enabled flag correctly" in {
      val config = FeatureConfigLoader.loadFromResource("/config/test_features.yaml")
      val enabled = config.features.find(_.name === "user_age").get
      val disabled = config.features.find(_.name === "disabled_feat").get
      assert(enabled.isEnabled === true)
      assert(disabled.isEnabled === false)
    }

    "parse cross features" in {
      val config = FeatureConfigLoader.loadFromResource("/config/test_features.yaml")
      val crosses = config.cross_features.get
      assert(crosses.size === 2)
      val active = crosses.find(_.name === "cross_age_gender").get
      val inactive = crosses.find(_.name === "cross_disabled").get
      assert(active.isEnabled === true)
      assert(inactive.isEnabled === false)
      assert(active.depends === Seq("user_age", "user_gender"))
    }

    "throw on missing resource" in {
      intercept[RuntimeException] {
        FeatureConfigLoader.loadFromResource("/config/nonexistent.yaml")
      }
    }
  }
}
