package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.CreateArray
import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.data.simple.{DoubleType, FloatType, IntegerType, LongType}
import es.us.idea.adt.data.chameleon.internal.dtf.aggregation._
import es.us.idea.adt.data.chameleon.internal.dtf.filter.Filter
import es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates._
import es.us.idea.adt.data.chameleon.internal.dtf.order.OrderBy
import es.us.idea.adt.data.chameleon.internal.dtf.transformation._
import es.us.idea.adt.data.chameleon.internal.dtf.udf.UDF

object dtfs {

  // TODO mucho ojo, he definido como ArrayEvaluable Iterate y CreateArray. Sin embargo, Select también puede entrar en esta
  // categoría. Tenerlo en cuenta y considerar dejar solo una posible definición de esta función que reciba solo un objeto Evaluable
  //def max(evals: Evaluable*): Max = new Max(new CreateArray(evals))
  def max(eval: Evaluable):Max = new Max(eval)

  //def min(evals: Evaluable*): Min = new Min(new CreateArray(evals))
  def min(eval: Evaluable): Min = new Min(eval)

  //def first(evals: Evaluable*): First = new First(new CreateArray(evals))
  def first(eval: Evaluable): First = new First(eval)

  //def last(evals: Evaluable*): Last = new Last(new CreateArray(evals))
  def last(eval: Evaluable): Last = new Last(eval)

  //def sum(evals: Evaluable*): Sum = new Sum(new CreateArray(evals))
  def sum(eval: Evaluable): Sum = new Sum(eval)

  //def avg(evals: Evaluable*): Avg = new Avg(new CreateArray(evals))
  def avg(eval: Evaluable): Avg = new Avg(eval)

  def count(eval: Evaluable): Count = new Count(eval)

  def filter(eval: Evaluable, predicate: Predicate) = new Filter(eval, predicate)

  def equal(eval: Evaluable, value: Any) = new Equal(eval, value)
  def notEqual(eval: Evaluable, value: Any) = new NotEqual(eval, value)
  def gt(eval: Evaluable, value: Any) = new GreaterThan(eval, value)
  def get(eval: Evaluable, value: Any) = new GreaterEqualThan(eval, value)
  def lt(eval: Evaluable, value: Any) = new LessThan(eval, value)
  def let(eval: Evaluable, value: Any) = new LessEqualThan(eval, value)
  def not(predicate: Predicate) = new Not(predicate)
  def and(predicate: Predicate*) = new And(predicate: _*)
  def or(predicate: Predicate*) = new Or(predicate: _*)

  def toInt(eval: Evaluable) = new ToInt(eval)
  def toDouble(eval: Evaluable) = new ToDouble(eval)
  def toFloat(eval: Evaluable) = new ToFloat(eval)
  def toLong(eval: Evaluable) = new ToLong(eval)
  def toString(eval: Evaluable) = new ToString(eval)
  def toDate(format: String)(eval: Evaluable) = new ToDate(eval, format)

  def orderBy(criteria: Evaluable, ascending: Boolean)(arrayEvaluable: Evaluable) = new OrderBy(arrayEvaluable, criteria, ascending)
  def orderBy(criteria: Evaluable)(arrayEvaluable: Evaluable) = new OrderBy(arrayEvaluable, criteria)

}
