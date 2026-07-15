package pipeline.serde

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row}

import featurizer.Featurizer

/**
 * Serializes training samples into Parquet columnar format.
 */
class ParquetRecord[T: ClassTag](createEncoder: () => Featurizer[T], max_dim: Long) extends Serializable {

  /** Encodes samples into Parquet columnar format and writes to `parquetPath`. */
  def writeParquet(spark: SparkSession,
                   trainingSample: RDD[(T, Boolean)],
                   parquetSchema: StructType,
                   posMap: collection.Map[(Int, Long), Int],
                   targetMap: collection.Map[Int, Int],
                   parquetPath: String
                  ): Unit = {
    val parquetFieldNames = parquetSchema.fieldNames.toSeq
    val parquetRows: RDD[Row] = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        val encoder = createEncoder()
        samples.flatMap(sample => {
          val (record, has_feature, has_target) = sampleToParquet(sample, encoder, posMap, targetMap)
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

  /** Encodes a single sample into a ParquetRecordData. Returns (record, has_feature, has_target). */
  private def sampleToParquet(sample: T,
                              encoder: Featurizer[T],
                              posMap: collection.Map[(Int, Long), Int],
                              targetMap: collection.Map[Int, Int]): (ParquetRecordData, Boolean, Boolean) = {
    val builder = ParquetRecordData.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, posMap, targetMap)
    (builder.build(), has_feature, has_target)
  }
}
