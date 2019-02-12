package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.CreateArray
import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.data.simple.{DoubleType, FloatType, IntegerType, LongType}
import es.us.idea.adt.data.chameleon.internal.dtf.aggregation.{First, Last, Max, Min}
import es.us.idea.adt.data.chameleon.internal.dtf.udf.UDF

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

  def scale(i: Int) = UDF((n: Any) =>{
    scaleFunction(i)(n)

  }, new IntegerType())

  def max(evals: Evaluable*): Max = new Max(new CreateArray(evals))
  def max(eval: ArrayEvaluable):Max = new Max(eval)

  def min(evals: Evaluable*): Min = new Min(new CreateArray(evals))
  def min(eval: ArrayEvaluable): Min = new Min(eval)

  def first(evals: Evaluable*): First = new First(new CreateArray(evals))
  def first(eval: ArrayEvaluable): First = new First(eval)

  def last(evals: Evaluable*): Last = new Last(new CreateArray(evals))
  def last(eval: ArrayEvaluable): Last = new Last(eval)

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
