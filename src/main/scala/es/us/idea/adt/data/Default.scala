package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}
import org.apache.spark.sql.types.DataType

/** This class represents a Default Data in the data structure
  *
  * @param value the default data value
  * @param dataType the type of the default Data value as a DataType SparkSQL object
  */
class Default(value: Any, dataType: DataType) extends Data {
  override def getValue(in: Any): Any = value

  override def getSchema(schm: ADTSchema): ADTSchema = new ADTDataType(dataType)

  override def getFirstLevelPath(): Seq[String] = Seq("")
}