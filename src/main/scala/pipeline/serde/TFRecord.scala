package pipeline.serde

import scala.reflect.ClassTag
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.tensorflow.example.Example

import featurizer.Featurizer

/** Serializes training samples into TFRecord (TensorFlow Example) format. */
class TFRecord[T: ClassTag](createEncoder: () => Featurizer[T], max_dim: Long) extends BaseRecord[T](createEncoder, max_dim) {
  /** Encodes samples into TensorFlow Example protobuf and writes TFRecord to `tfRecordPath`. */
  def write(trainingSample: RDD[(T, Boolean)],
            /** HashMap[(f_index, hash), pos] */
            posMap: collection.Map[(Int, Long), Int],
            /** HashMap[target, pos] */
            targetMap: collection.Map[Int, Int],
            tfRecordPath: String
           ): Unit = {
    trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        // Factory function ensures each Spark partition gets its own featurizer instance, avoiding shared mutable state
        val encoder = createEncoder()
        samples.flatMap(sample => {
          val (example, has_feature, has_target) = encode(sample, encoder, posMap, targetMap)
          if (has_feature && has_target) {
            Some(example)
          } else {
            None
          }
        })
      })
      .map(example => {
        val key = new BytesWritable(example.toByteArray)
        val value = NullWritable.get()
        (key, value)
      })
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](tfRecordPath)
  }

  /** Encodes a single sample into a TensorFlow Example. Returns (example, has_feature, has_target). */
  def encode(sample: T,
             encoder: Featurizer[T],
             posMap: collection.Map[(Int, Long), Int],
             targetMap: collection.Map[Int, Int]): (Example, Boolean, Boolean) = {
    val builder = Example.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, posMap, targetMap)
    (builder.build(), has_feature, has_target)
  }
}
