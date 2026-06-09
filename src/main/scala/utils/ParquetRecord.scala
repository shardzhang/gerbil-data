package utils

import org.tensorflow.example.Example
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * @author shard zhang
 * @date 2026/6/3 14:15
 * @note ParquetRecord
 */
case class ParquetRecord(columns: mutable.Map[String, Any]) extends Serializable {
  def to_seq(column_names: Seq[String]): Seq[Any] = {
    column_names.map(name => columns.getOrElse(name, null))
  }
}

object ParquetRecord {
  def newBuilder(): ParquetRecordBuilder = new ParquetRecordBuilder()

  // Builder内部类，链式set，最终build生成实例
  class ParquetRecordBuilder private[ParquetRecord]() {
    private val innerMap = mutable.Map.empty[String, Any]

    // 链式添加字段
    def put(k: String, v: Any): ParquetRecordBuilder = {
      innerMap.put(k, v)
      this
    }

    // 批量导入map
    def putAll(map: mutable.Map[String, Any]): ParquetRecordBuilder = {
      innerMap ++= map
      this
    }

    // 最终build生成ParquetRecord
    def build(): ParquetRecord = ParquetRecord(innerMap)

    def clear(): ParquetRecordBuilder = {
      innerMap.clear()
      this
    }
  }

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
