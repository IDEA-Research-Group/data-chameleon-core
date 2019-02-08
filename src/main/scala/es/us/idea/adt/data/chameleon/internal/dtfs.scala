package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.data.simple.{DoubleType, FloatType, IntegerType, LongType}

object dtfs {

  private def scaleFunction(i: Int)(n: Any): Option[Any] = {
    n match {
      case Some(x) => scaleFunction(i)(n)
      case int: Int => Some(int * i)
      case double: Double => Some(double * i)
      case float: Float => Some(float * i)
      case long: Long => Some(long * i)
      case _ => None
    }
  }

  def scale(i: Int) = DTF((n: Any) =>{
    scaleFunction(i)(n)

  }, (dt: DataType) => {
    dt match {
      case integerType: IntegerType => integerType
      case doubleType: DoubleType => doubleType
      case floatType: FloatType => floatType
      case longType: LongType => longType
      case _ => throw new Exception("Scale DTF must be applied to one of these data types: IntegerType, DoubleType, FloatType or LongType")
    }
  })

  //def max = DTF((seq: Seq[Any]) => {
  //  // todo especificar criterio de ordenacion
  //  seq.max
  //}, (dt: DataType) => {
  //  dt match {
  //    case dt: ArrayType => dt.getElementDataType
  //    case _ => throw new Exception("max DTF must be applied to a list")
  //  }
  //})






}
