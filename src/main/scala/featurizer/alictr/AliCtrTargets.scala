package featurizer.alictr

import featurizer.RawTarget

class AliCtrLabel extends RawTarget[AliCtrSample] {
  override def parse(sample: AliCtrSample): RawTarget[AliCtrSample] = {
    target = sample.label.toFloat
    this
  }
}

class AliCtrTarget extends RawTarget[AliCtrSample] {
  override def parse(sample: AliCtrSample): RawTarget[AliCtrSample] = {
    target = sample.target.toFloat
    this
  }
}

class AliCtrRating extends RawTarget[AliCtrSample] {
  override def parse(sample: AliCtrSample): RawTarget[AliCtrSample] = {
    target = sample.clk.toFloat
    this
  }
}
