package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}
import org.apache.spark.sql.types._
import utils._

class TypedData(data: Data, dataType: DataType) extends Data {
  override def getValue(in: Any): Any = {
    val value = data.getValue(in)
    dataType match {
      case _: DoubleType =>  println("double"); asDouble(value)
      case _: IntegerType => println("integer"); asInt(value)
      case _: StringType =>  println("string"); asString(value)
      case _: LongType =>    println("long"); asLong(value)
      case _ => None
    }
  }

  override def getSchema(schm: ADTSchema): ADTSchema = new ADTDataType(dataType)
}