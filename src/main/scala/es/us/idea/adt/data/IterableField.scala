package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import utils._

class IterableField(path: String, dataUnion: DataUnion) extends Data {
  override def getValue(in: Any): Any = {

    val processMap = (m: Map[String, Any]) => {
      recursiveGetValueFromPath(path, m)
        .map(s => s match {
          case seq: Seq[Any] => seq.map(v => dataUnion.getValue(v))
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
        case arrayType: ArrayType => dataUnion.getSchema(new ADTDataType(arrayType.elementType)) match {
          case _adtDataType: ADTDataType => new ADTDataType(DataTypes.createArrayType(_adtDataType.get))
          case _ => throw new Exception("El campo anidado no devolvió un DataType")
        }
        case _ => throw new Exception("Has llamado a IterableField y no le has pasado un IterableField!!!!!!!!!!!")
      }
    }
  }
}