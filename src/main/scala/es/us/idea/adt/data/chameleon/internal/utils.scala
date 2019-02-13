package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple._

import scala.util.Try

object utils {

  // Aquí irían las funciones para unwrap valores y transformarlos a un tipo determinado

  def unwrap(a: Any): Any = {
    a match {
      case Some(x) => x
      case null => None
      case _ => a
    }
  }

  // TODO IMPORTANTE: IMPLEMENTAR MECANISMOS PARA QUE EL VALOR ORIGINAL NO SE MODIFIQUE.
  // todo ejemplo: si entra un double y hay que convertirlo a int porque asi lo dicta dataType, una vez que se haya operado con el hay que volverlo a pasar a double
  def ensureDataType(a: Any, dataType: DataType): Any = {
    val unwraped = unwrap(a)
    val castValue =
      dataType match {
        case _: IntegerType => TypeConversions.asInt(unwraped)
        case _: DoubleType => TypeConversions.asDouble(unwraped)
        case _: LongType => TypeConversions.asLong(unwraped)
        case _: FloatType => TypeConversions.asFloat(unwraped)
        case _: StringType => TypeConversions.asString(unwraped)
        case _: DateType => throw new Exception("DateType conversion not implemented yet")
        case _: BooleanType => throw new Exception("BooleanType conversion not implemented yet")
        case other => throw new Exception(s"Unknown data type $other")
      }
    castValue.getOrElse(None)
  }

  def ensureNumber(a: Any): Option[Double] = TypeConversions.asDouble(unwrap(a))

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
            case b: Boolean => Some(if(b) 1.0 else 0.0)
            case _ => None
          }
      f(unwrap(value))
    }

    def asInt(value: Any): Option[Int] = asDouble(value).map(a => a.toInt)

    def asString(value: Any): Option[String] = Some(value.toString)

    def asLong(value: Any): Option[Long] = asDouble(value).map(a => a.toLong)

    def asFloat(value: Any): Option[Float] = asDouble(value).map(a => a.toFloat)

  }


}
