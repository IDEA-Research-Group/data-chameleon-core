package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.types._
import utils._

class DataModifier(data: Data, operation: (Any => Option[Any], Option[DataType])) extends Data {
  override def getValue(in: Any): Any = operation._1(data.getValue(in))

  override def getSchema(schm: ADTSchema): ADTSchema = {
    operation._2 match {
      case None => data.getSchema(schm)
      case Some(x) => new ADTDataType(x)
    }
  }

  override def getFirstLevelPath(): Seq[String] = data.getFirstLevelPath()
}