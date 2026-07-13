package featurizer.mobilerec

import featurizer.RawTarget

class MobileRecTarget extends RawTarget[MobileRecSample] {
  override def parse(sample: MobileRecSample): RawTarget[MobileRecSample] = {
    target = sample.target
    this
  }
}

class MobileRecLabel extends RawTarget[MobileRecSample] {
  override def parse(sample: MobileRecSample): RawTarget[MobileRecSample] = {
    target = if (sample.rating > 3) 1.0F else 0.0F
    this
  }
}

class MobileRecRating extends RawTarget[MobileRecSample] {
  override def parse(sample: MobileRecSample): RawTarget[MobileRecSample] = {
    target = sample.rating
    this
  }
}
