package pipeline.serde

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.tensorflow.example.Example

import featurizer.Featurizer

/** A serializable record backed by a mutable map of column name -> value. */
case class ParquetRecordData(columns: mutable.Map[String, Any]) extends Serializable {
  def to_seq(column_names: Seq[String]): Seq[Any] = {
    column_names.map(name => columns.getOrElse(name, null))
  }
}

/** Builder and converter for ParquetRecordData. */
object ParquetRecordData {
  def newBuilder(): Builder = new Builder()

  class Builder private[ParquetRecordData]() {
    private val innerMap = mutable.Map.empty[String, Any]

    def put(k: String, v: Any): Builder = {
      innerMap.put(k, v)
      this
    }

    def putAll(map: mutable.Map[String, Any]): Builder = {
      innerMap ++= map
      this
    }

    def build(): ParquetRecordData = ParquetRecordData(innerMap)

    def clear(): Builder = {
      innerMap.clear()
      this
    }
  }

  /** Converts a TensorFlow Example proto into a ParquetRecordData. */
  def from_example(example: Example): ParquetRecordData = {
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
    ParquetRecordData(columns)
  }
}

/**
 * Serializes training samples into Parquet columnar format.
 */
class ParquetRecord[T: ClassTag](createEncoder: () => Featurizer[T], max_dim: Long) extends BaseRecord[T](createEncoder, max_dim) {
  override def write(trainingSample: RDD[(T, Boolean)],
                     posMap: collection.Map[(Int, Long), Int],
                     targetMap: collection.Map[Int, Int],
                     path: String): Unit = {
    val encoder = createEncoder()
    val fieldInfo = encoder.getFieldInfo()
    val schema = buildParquetSchema(fieldInfo)
    val parquetFieldNames = schema.fieldNames.toSeq

    val parquetRows: RDD[Row] = trainingSample
      .map { case (sample, _) => sample }
      .mapPartitions(samples => {
        val encoder = createEncoder()
        samples.flatMap(sample => {
          val (record, has_feature, has_target) = encode(sample, encoder, posMap, targetMap)
          if (has_feature && has_target) {
            Some(Row.fromSeq(record.to_seq(parquetFieldNames)))
          } else {
            None
          }
        })
      })

    SparkSession.active
      .createDataFrame(parquetRows, schema)
      .write
      .mode("overwrite")
      .parquet(path)
  }

  private def buildParquetSchema(fieldInfo: ArrayBuffer[(String, Int)]): StructType = {
    val fields = new ArrayBuffer[StructField]()
    fields.append(StructField("target", FloatType, nullable = true))
    for ((f_name, _) <- fieldInfo) {
      fields.append(StructField(f_name + "_raw", ArrayType(StringType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_index", ArrayType(LongType, containsNull = false), nullable = true))
      fields.append(StructField(f_name + "_value", ArrayType(FloatType, containsNull = false), nullable = true))
    }
    StructType(fields)
  }

  /** Encodes a single sample into a ParquetRecordData. Returns (record, has_feature, has_target). */
  def encode(sample: T,
             encoder: Featurizer[T],
             posMap: collection.Map[(Int, Long), Int],
             targetMap: collection.Map[Int, Int]): (ParquetRecordData, Boolean, Boolean) = {
    val builder = ParquetRecordData.newBuilder()
    val (has_feature, has_target) = encoder.encode(sample, max_dim, builder, posMap, targetMap)
    (builder.build(), has_feature, has_target)
  }
}
