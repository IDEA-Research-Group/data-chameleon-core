package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}
import org.apache.spark.sql.types.DataType

class Default(value: Any, dataType: DataType) extends Data {
  override def getValue(in: Any): Any = value

  override def getSchema(schm: ADTSchema): ADTSchema = new ADTDataType(dataType)
}