package featurizer.alictr

import utils.MurmurHash3
import featurizer.core.{CategoricalFeature, RawFeature}

import scala.collection.mutable.ArrayBuffer

trait UserHistoryAdSeqLike {
  this: CategoricalFeature[AliCtrSample] =>

  override def parse(sample: AliCtrSample): RawFeature = {
    val seq = sample.user_history_ads
    for (i <- 0 until Math.min(200, seq.size)) {
      val p = new MurmurHash3.LongPair()
      MurmurHash3.murmurhash3_x64_128(seq(i)._1.getBytes, 0, seq(i)._1.length, SEED, p)
      raw_list.append(seq(i)._1)
      feature_list.append(p.val1)
      value_list.append(seq(i)._2.toFloat)
    }
    this
  }
}

class AliCtrUserHistoryAdSeq(f_i: Int, f_n: String) extends CategoricalFeature[AliCtrSample](f_i, f_n) with UserHistoryAdSeqLike
