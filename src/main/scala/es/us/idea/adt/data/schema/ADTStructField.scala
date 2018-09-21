package es.us.idea.adt.data.schema

import org.apache.spark.sql.types.StructField

/** ADTSchema that wraps the StructField Apache Spark class. StructField is a schema element composed by a name
  * and a DataType.
  *
  * @param structField the wrapped StructField object from the SparkSQL library
  */
class ADTStructField(structField: StructField) extends ADTSchema {
  def get = structField


  override def equals(that: Any): Boolean = {
    that match {
      case that: ADTStructField => that.get.equals(this.get)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    41 * (41 + structField.hashCode())
  }
}