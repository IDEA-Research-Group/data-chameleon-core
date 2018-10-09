package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

import scala.util.Try

/** A set of functions to provide basic functionality for the data and schema extraction
  *
  */
object utils {

  /** Return the value specified by a path in the given Map[String, Any]
    *
    * @param path route to the value to extract.
    * @param map data structure where the data will be found
    * @return the value specified by the path param
    */
  def recursiveGetValueFromPath(path: String, map: Map[String, Any]): Option[Any] = {
    // TODO refactorizar. Cambiar el parametro path por una lista de cadenas. Evitar los asInstanceOf
    if (path.contains(".")) {
      val keySplt = path.split('.')
      if (map.contains(keySplt.head)) {
        return recursiveGetValueFromPath(keySplt.tail.mkString("."), map(keySplt.head).asInstanceOf[Map[String, Any]])
      } else {
        return None
      }
    } else {
      return map.get(path)
    }
  }

  /** Find a sub-schema in a ADTSchema object
    *
    * @param path route to the schema to be extracted
    * @param schema the schema to analyse
    * @return the extracted schema
    */
  def recursiveGetSchemaFromPath(path: Seq[String], schema: ADTSchema): ADTSchema = {

    val processStructType = (key: String, st: StructType) => {
      st.fields.find(_.name == key)
        .map(_structField => if(path.length > 1){recursiveGetSchemaFromPath(path.tail, new ADTStructField(_structField))} else new ADTStructField(_structField))
    }

    val processDataType = (key: String, dataType: DataType) => {
      dataType match {
        case structType: StructType => processStructType(key, structType)
        case _ => throw new Exception("Error en el tipo de dato de entrada")
      }
    }

    path.headOption.flatMap(k => {
      schema match {
        case adtStructField: ADTStructField => processDataType(k, adtStructField.get.dataType)
        case adtDataType: ADTDataType => processDataType(k, adtDataType.get)
        case _ => throw new Exception("Tipo de dato no reconocible")
      }
    }).getOrElse(schema)
  }

  /** Loops the input data structure and replaces every Map[String, Any] with a Row
    *
    * @param in the source data structure
    * @return the resulting data structure with all the Map[String, Any] replaced with a Row
    */
  def findAndReplaceMaps(in: Any): Any = {

    val process = (a: Any) => {
      a match {
        case map: Map[String, Any] => Row.apply(map.toArray.map(t => findAndReplaceMaps(t._2)): _*)
        case seq: Seq[Any] => seq.map(e => findAndReplaceMaps(e))
        case any => any
      }
    }

    in match {
      case Some(x: Any) => process(x)
      case _ => process(in)
    }
  }


  implicit class SequencesAndOptions(seq: Seq[Any]) {

    def applyOperationOpt(f: (Double, Double) => Double, initialValue: Option[Double]=None): Option[Double] = {
      initialValue match {
        case None => seq.map(TypeConversions.asDouble).reduceLeft((x, y) => x.flatMap(_x => y.map(_y => f(_x, _y))))
        case _ => seq.foldLeft(initialValue){ case (acc, el) => TypeConversions.asDouble(el).flatMap(value => acc.map(ac => f(ac, value)))}
      }
    }

    def sumOpt()(implicit ev: Numeric[Double])= {
      applyOperationOpt(ev.plus, Some(ev.zero))
    }

    def maxOpt()(implicit ev: Numeric[Double]) = {
      applyOperationOpt(ev.max)
    }

    def minOpt()(implicit ev: Numeric[Double]) = {
      applyOperationOpt(ev.min)
    }

    def avgOpt()(implicit ev: Numeric[Double]) = {
      sumOpt().map(_/seq.length)
    }

  }

  /**
    * Type conversions
    * */
  object TypeConversions {

    def asDouble(value: Any): Option[Double] = {
      val f =
        (a: Any) =>
          a match {
            case s: String => Try(s.toDouble).toOption
            case i: Int => Some(i.toDouble)
            case l: Long => Some(l.toDouble)
            case f: Float => Some(f.toDouble)
            case d: Double => Some(d)
            case _ => None
          }

      value match {
        case Some(a: Any) => f(a)
        case _ => f(value)
      }

    }

    def asInt(value: Any): Option[Int] = asDouble(value).map(a => a.toInt)

    def asString(value: Any): Option[String] = Some(value.toString)

    def asLong(value: Any): Option[Long] = asDouble(value).map(a => a.toLong)

  }

  object Transformations{

    def times(value: Any, literal: Either[Int, Double]) = {

      val f =
        (a: Any) =>
          a match {
            case s: String =>
              literal match {
                case Left(i) => Try((s.toDouble * i).toString).toOption
                case Right(d) => Try((s.toDouble * d).toString).toOption
              }
            case int: Int =>
              literal match {
                case Left(i) => Some(int * i)
                case Right(d) => Some(int * d)
              }
            case l: Long =>
              literal match {
                case Left(i) => Some(l * i)
                case Right(d) => Some(l * d)
              }
            case f: Float =>
              literal match {
                case Left(i) => Some((f * i).toDouble)
                case Right(d) => Some(f * d)
              }
            case double: Double =>
              literal match {
                case Left(i) => Some(double * i)
                case Left(d) => Some(double * d)
              }
            case _ => None
          }

      value match {
        case Some(a: Any) => f(a)
        case _ => f(value)
      }
    }



  }

}
