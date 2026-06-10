package featurizer.ml1m

import featurizer.ml1m.ML1MSample
import featurizer.core.RawTarget

/** Multi-class target: uses the raw rating value (1-5) directly as the class ID. */
class Target extends RawTarget[ML1MSample] {
  override def parse(sample: ML1MSample): RawTarget[ML1MSample] = {
    target = sample.target
    this
  }
}

/** Binary target: rating >= 3 → positive (1.0), rating < 3 → negative (0.0). */
class Label extends RawTarget[ML1MSample] {
  override def parse(sample: ML1MSample): RawTarget[ML1MSample] = {
    target = if (sample.rating >= 3) {
      1.0F
    } else {
      0.0F
    }
    this
  }
}

/** Regression target: uses the raw rating value directly as a continuous float. */
class Rating extends RawTarget[ML1MSample] {
  override def parse(sample: ML1MSample): RawTarget[ML1MSample] = {
    target = sample.rating
    this
  }
}
