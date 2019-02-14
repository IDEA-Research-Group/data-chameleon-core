package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.{DataType, SimpleType}
import es.us.idea.adt.data.chameleon.data.complex.{ArrayType, StructType}
import es.us.idea.adt.data.chameleon.internal.utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable

object SparkDataConversor {
  def spark2chameleon(row: Row, dsSchema: Option[org.apache.spark.sql.types.StructType] = None):Map[String, Any] = {

    def format(schema: Seq[org.apache.spark.sql.types.StructField], row: Row): Map[String, Any] = {
      var result:Map[String, Any] = Map()

      schema.foreach(s => //println(s.dataType)
        s.dataType match {
          case org.apache.spark.sql.types.ArrayType(elementType, _)=> val thisRow = row.getAs[mutable.WrappedArray[Any]](s.name); result = result ++ Map(s.name -> formatArray(elementType, thisRow))
          case org.apache.spark.sql.types.StructType(structFields)=> val thisRow = row.getAs[Row](s.name); result = result ++ Map( s.name -> format(thisRow.schema, thisRow))
          case _ => result = result ++ Map(s.name -> row.getAs(s.name))
        }
      )
      return result
    }

    def formatArray(elementType: org.apache.spark.sql.types.DataType, array: mutable.WrappedArray[Any]): Seq[Any] = {
      elementType match {
        case org.apache.spark.sql.types.StructType(structFields) => array.map(e => format(structFields, e.asInstanceOf[Row]))
        case org.apache.spark.sql.types.ArrayType(elementType2, _) => array.map(e => formatArray(elementType2, e.asInstanceOf[mutable.WrappedArray[Any]]))
        case _ => array
      }
    }

    if(dsSchema.isDefined) format(dsSchema.get, new GenericRowWithSchema(row.toSeq.toArray, dsSchema.get) ) else format(row.schema, row)

  }



  def chameleon2spark(data: Any, schema: DataType): Any = {

    def format(data: Any, schema: DataType): Any = {
      val unwrappedData = utils.unwrap(data)
      schema match {
        case structType: StructType =>
          unwrappedData match {
            case map: Map[String, Any] =>
              Row.apply(structType.getAttributes.map( a => {
                format(utils.unwrap(map.get(a.getName)), a.getDataType)
              }))
            case _ => None
          }
        case arrayType: ArrayType =>
          unwrappedData match {
            case seq: Seq[Any] => seq.map(element => format(utils.unwrap(element), arrayType.getElementDataType))
            case _ => None
          }
        case simpleType: SimpleType => unwrappedData
      }
    }

    schema match {
      case st: StructType => {
        format(data, st)
      }
      case other => throw new Exception(s"Parent DataType must be StructType, instead it was $other")
    }
  }

}
