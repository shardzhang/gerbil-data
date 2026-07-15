package pipeline.serde

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** A serializable record backed by a mutable map of column name -> value. */
case class ParquetRecordData(columns: mutable.Map[String, Any]) extends Serializable {
  /** Extracts column values in the order given by `column_names`. */
  def to_seq(column_names: Seq[String]): Seq[Any] = {
    column_names.map(name => columns.getOrElse(name, null))
  }
}

/** Builder and converter for ParquetRecordData. */
object ParquetRecordData {
  def newBuilder(): Builder = new Builder()

  /** Builder with chainable `put`/`putAll` and terminal `build`. */
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
}
