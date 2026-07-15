package pipeline.serde

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

import featurizer.Featurizer

/**
 * Base class for training sample serialization writers.
 *
 * Defines a unified write interface that abstracts over output format differences
 * (TFRecord vs Parquet), so the pipeline driver can call the same method regardless
 * of the target format. Concrete subclasses implement the format-specific encoding
 * and I/O logic.
 *
 * Subclasses:
 *   - [[TFRecord]]     — writes samples as TensorFlow Example protobuf via Hadoop TFRecord
 *   - [[ParquetRecord]] — writes samples as Parquet columnar format via Spark DataFrame
 *
 * Instances are created through [[pipeline.Pipeline.createRecord]] which selects the
 * appropriate subclass based on the `--output_format` CLI parameter.
 *
 * @tparam T the raw sample type (e.g. ML1MSample, MobileRecSample, AliCtrSample)
 * @param createEncoder factory function that creates a new Featurizer per Spark partition
 * @param max_dim      maximum hash dimension (typically 2^60 for 64-bit embedding space)
 */
abstract class BaseRecord[T: ClassTag](val createEncoder: () => Featurizer[T], val max_dim: Long) extends Serializable {

  /**
   * Encodes and writes training samples to the specified path in the subclass-specific format.
   *
   * @param trainingSample RDD of (sample, parseSuccess) pairs
   * @param posMap         HashMap[(f_index, hash), embedding_position] — vocabulary lookup
   * @param targetMap      HashMap[target, dense_index] — target re-indexing (null if unused)
   * @param path           output directory path
   */
  def write(trainingSample: RDD[(T, Boolean)],
            posMap: collection.Map[(Int, Long), Int],
            targetMap: collection.Map[Int, Int],
            path: String
           ): Unit
}
