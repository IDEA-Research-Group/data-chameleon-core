package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}
import org.apache.spark.sql.types._
import utils._

class TypedData(data: Data, dataType: DataType) extends Data {
  override def getValue(in: Any): Any = {
    val value = data.getValue(in)
    dataType match {
      case _: DoubleType => asDouble(value)
      case _: IntegerType => asInt(value)
      case _: StringType => asString(value)
      case _: LongType => asLong(value)
      case _ => None
    }
  }

  override def getSchema(schm: ADTSchema): ADTSchema = new ADTDataType(dataType)

  override def getFirstLevelPath(): Seq[String] = data.getFirstLevelPath()
}