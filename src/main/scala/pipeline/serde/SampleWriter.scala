package pipeline.serde

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.tensorflow.example.Example

import scala.reflect.ClassTag
import featurizer.core.Featurizer

/**
 * Training sample serializer — writes encoded features to TFRecord or Parquet
 */

/** Serializes training samples into TFRecord (TensorFlow Example) or Parquet format. */
class SampleWriter[T: ClassTag](createEncoder: () => Featurizer[T], max_dim: Long) extends Serializable {

  /** Encodes samples into Parquet columnar format and writes to `parquetPath`. */
  def writeParquet(trainingSample: RDD[(T, Boolean)],
                   spark: SparkSession,
                   parquetSchema: StructType,
                   parquetFieldNames: Seq[String],
                   posMapLocalImmutable: collection.Map[(Int, Long), Int],
                   targetMapImmutable: collection.Map[Int, Int],
                   parquetPath: String
                  ): Unit = {
    val parquetRows: RDD[Row] = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        // Factory function ensures each Spark partition gets its own featurizer instance, avoiding shared mutable state
        val encoder = createEncoder()
        samples.flatMap(sample => {
          val (record, has_feature, has_target) = parseParquet(sample, encoder, posMapLocalImmutable, targetMapImmutable)
          if (has_feature && has_target) {
            Some(Row.fromSeq(record.to_seq(parquetFieldNames)))
          } else {
            None
          }
        })
      })

    spark.createDataFrame(parquetRows, parquetSchema)
      .write
      .mode("overwrite")
      .parquet(parquetPath)
  }

  /** Encodes samples into TensorFlow Example protobuf and writes TFRecord to `tfRecordPath`. */
  def writeTfrecord(trainingSample: RDD[(T, Boolean)],
                    posMapLocalImmutable: collection.Map[(Int, Long), Int],
                    targetMap: collection.Map[Int, Int],
                    tfRecordPath: String
                   ): Unit = {
    trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        // Factory function ensures each Spark partition gets its own featurizer instance, avoiding shared mutable state
        val encoder = createEncoder()
        samples.flatMap(sample => {
          val (example, has_feature, has_target) = parseTfrecord(sample, encoder, posMapLocalImmutable, targetMap)
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
  private def parseTfrecord(sample: T,
                            encoder: Featurizer[T],
                            pos_map: collection.Map[(Int, Long), Int],
                            target_map: collection.Map[Int, Int]): (Example, Boolean, Boolean) = {
    val builder = Example.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }

  /** Encodes a single sample into a ParquetRecord. Returns (record, has_feature, has_target). */
  private def parseParquet(sample: T,
                           encoder: Featurizer[T],
                           pos_map: collection.Map[(Int, Long), Int],
                           target_map: collection.Map[Int, Int]): (ParquetRecord, Boolean, Boolean) = {
    val builder = ParquetRecord.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }
}
