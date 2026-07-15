package pipeline.serde

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.tensorflow.example.Example

import featurizer.Featurizer

/**
 * ParquetRecord
 */
/** A serializable record backed by a mutable map of column name -> value. */
case class ParquetRecord2(columns: mutable.Map[String, Any]) extends Serializable {
  /** Extracts column values in the order given by `column_names`. */
  def to_seq(column_names: Seq[String]): Seq[Any] = {
    column_names.map(name => columns.getOrElse(name, null))
  }
}

/** Builder and converter for ParquetRecord. */
object ParquetRecord {
  def newBuilder(): ParquetRecordBuilder = new ParquetRecordBuilder()

  /** Builder with chainable `put`/`putAll` and terminal `build`. */
  class ParquetRecordBuilder private[ParquetRecord]() {
    private val innerMap = mutable.Map.empty[String, Any]

    /** Sets a single field. */
    def put(k: String, v: Any): ParquetRecordBuilder = {
      innerMap.put(k, v)
      this
    }

    /** Bulk-imports all entries from a map. */
    def putAll(map: mutable.Map[String, Any]): ParquetRecordBuilder = {
      innerMap ++= map
      this
    }

    /** Produces the final ParquetRecord. */
    def build(): ParquetRecord2 = ParquetRecord2(innerMap)

    /** Clears all accumulated fields and resets the builder. */
    def clear(): ParquetRecordBuilder = {
      innerMap.clear()
      this
    }
  }

  /** Converts a TensorFlow Example proto into a ParquetRecord. */
  def from_example(example: Example): ParquetRecord2 = {
    val columns = new mutable.HashMap[String, Any]()
    for ((name, feature) <- example.getFeatures.getFeatureMap.asScala) {
      if (name == "target") {
        columns.put(name, feature.getFloatList.getValue(0))
      } else if (name.endsWith("_raw")) {
        columns.put(name, feature.getBytesList.getValueList.asScala.map(_.toByteArray))
      } else if (name.endsWith("_index")) {
        columns.put(name, feature.getInt64List.getValueList.asScala.map(_.toLong))
      } else if (name.endsWith("_value")) {
        columns.put(name, feature.getFloatList.getValueList.asScala.map(_.toFloat))
      }
    }
    ParquetRecord2(columns)
  }
}


/**
 * Training sample serializer — writes encoded features to TFRecord or Parquet
 */

/** Serializes training samples into TFRecord (TensorFlow Example) or Parquet format. */
class ParquetRecord[T: ClassTag](createEncoder: () => Featurizer[T], max_dim: Long) extends Serializable {

  /** Encodes samples into Parquet columnar format and writes to `parquetPath`. */
  def writeParquet(spark: SparkSession,
                   trainingSample: RDD[(T, Boolean)],
                   parquetSchema: StructType,
                   posMapLocalImmutable: collection.Map[(Int, Long), Int],
                   targetMapImmutable: collection.Map[Int, Int],
                   parquetPath: String
                  ): Unit = {
    val parquetFieldNames = parquetSchema.fieldNames.toSeq
    val parquetRows: RDD[Row] = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        // Factory function ensures each Spark partition gets its own featurizer instance, avoiding shared mutable state
        val encoder = createEncoder()
        samples.flatMap(sample => {
          val (record, has_feature, has_target) = sampleToParquet(sample, encoder, posMapLocalImmutable, targetMapImmutable)
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

  /** Encodes a single sample into a ParquetRecord. Returns (record, has_feature, has_target). */
  def sampleToParquet(sample: T,
                      encoder: Featurizer[T],
                      pos_map: collection.Map[(Int, Long), Int],
                      target_map: collection.Map[Int, Int]): (ParquetRecord2, Boolean, Boolean) = {
    val builder = ParquetRecord.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, pos_map, target_map)
    (builder.build(), has_feature, has_target)
  }
}
