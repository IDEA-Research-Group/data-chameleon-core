package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import utils._

class BasicField(path: String) extends Data {
  override def getValue(in: Any): Any = {
    in match {
      case m: Map[String, Any] =>  recursiveGetValueFromPath(path, m) //ADTDSL.recursiveGetValueFromPath(path,in.asInstanceOf[Map[String, Any]])
      case _ => None
    }
  }

  override def getSchema(schema: ADTSchema): ADTSchema = {

    val resSchm = recursiveGetSchemaFromPath(path.split('.'), schema)

    resSchm match {
      case adtStructField: ADTStructField => new ADTDataType(adtStructField.get.dataType)
      case _ => resSchm
    }
  }
}