package pipeline.serde

import org.tensorflow.example.Example
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * ParquetRecord
 */
/** A serializable record backed by a mutable map of column name -> value. */
case class ParquetRecord(columns: mutable.Map[String, Any]) extends Serializable {
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
    def build(): ParquetRecord = ParquetRecord(innerMap)

    /** Clears all accumulated fields and resets the builder. */
    def clear(): ParquetRecordBuilder = {
      innerMap.clear()
      this
    }
  }

  /** Converts a TensorFlow Example proto into a ParquetRecord. */
  def from_example(example: Example): ParquetRecord = {
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
    ParquetRecord(columns)
  }
}
