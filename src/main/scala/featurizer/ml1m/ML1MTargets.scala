package featurizer.ml1m

import featurizer.ml1m.ML1MSample
import featurizer.core.RawTarget

class Target extends RawTarget[ML1MSample] {
  override def parse(sample: ML1MSample): RawTarget[ML1MSample] = {
    target = sample.target
    this
  }
}

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

class Rating extends RawTarget[ML1MSample] {
  override def parse(sample: ML1MSample): RawTarget[ML1MSample] = {
    target = sample.rating
    this
  }
}
