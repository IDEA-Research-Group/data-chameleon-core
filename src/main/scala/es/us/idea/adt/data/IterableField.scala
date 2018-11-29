package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import utils._

/** An IterableField is composed of a field which contains a sequence of Data and the Data Union that will be a part of
  * the field.
  *
  * @param path path to the iterable field
  * @param data sequence
  */
class IterableField(path: String, data: Data) extends DataUnion {
  override def getValue(in: Any): Any = {

    val processMap = (m: Map[String, Any]) => {
      recursiveGetValueFromPath(path, m)
        .map(s => s match {
          case seq: Seq[Any] => seq.map(v => data.getValue(v))
          case _ => None
        }
        )
    }

    in match {
      case m: Map[String, Any] => processMap(m)
      case _ => None
    }
  }

  //override def getSchema(schm: ADTSchema): ADTSchema = ???
  override def getSchema(schm: ADTSchema): ADTSchema = {

    val pathSchm = recursiveGetSchemaFromPath(path.split('.'), schm)

    pathSchm match {
      case adtStructField: ADTStructField => adtStructField.get.dataType match {
        case arrayType: ArrayType => data.getSchema(new ADTDataType(arrayType.elementType)) match {
          case _adtDataType: ADTDataType => new ADTDataType(DataTypes.createArrayType(_adtDataType.get))
          case _ => throw new Exception("El campo anidado no devolvió un DataType")
        }
        case _ => throw new Exception("Has llamado a IterableField y no le has pasado un IterableField!!!!!!!!!!!")
      }
    }
  }

  override def getFirstLevelPath(): Seq[String] = Seq(path)
}