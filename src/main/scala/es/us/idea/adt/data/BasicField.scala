package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import utils._

/** This class represents a leaf in the data structure.
  *
  * @param path the path to the data to extract
  */
class BasicField(path: String) extends Data {

  /**
    *
    * @param in the source data structure where the value must be found
    * @return the extracted Data value
    */
  override def getValue(in: Any): Any = {
    in match {
      case m: Map[String, Any] => recursiveGetValueFromPath(path, m)
      case _ => None
    }
  }

  /**
    *
    * @param schema the source schema
    * @return the resulting schema
    */
  override def getSchema(schema: ADTSchema): ADTSchema = {

    val resSchm = recursiveGetSchemaFromPath(path.split('.'), schema)

    resSchm match {
      case adtStructField: ADTStructField => new ADTDataType(adtStructField.get.dataType)
      case _ => resSchm
    }
  }

  /**
    *
    * @return the first level path that points at this Data
    */
  override def getFirstLevelPath(): Seq[String] = Seq(path)

}