package es.us.idea.adt.data.schema

import org.apache.spark.sql.types.DataType

/** ADTSchema that wraps the SparkSQL DataType object, representing a particular type of data for a field.
  *
  * @param dataType the wrapped DataType object from the SparkSQL library
  */
class ADTDataType(dataType: DataType) extends ADTSchema {
  def get = dataType

  override def equals(that: Any): Boolean = {
    that match {
      case that: ADTDataType => that.get.equals(this.get)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    41 * (41 + dataType.hashCode())
  }
}