package pipeline.serde

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

import featurizer.Featurizer

abstract class BaseRecord[T: ClassTag](val createEncoder: () => Featurizer[T], val max_dim: Long) extends Serializable {
  def write(trainingSample: RDD[(T, Boolean)],
            posMap: collection.Map[(Int, Long), Int],
            targetMap: collection.Map[Int, Int],
            path: String): Unit
}
