package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.internal.Evaluable

class Sum(eval: Evaluable) extends ArithmeticAggregationDTF(eval){
  override def getInitialValue: Double = 0.0

  override def operation(a: Double, b: Double): Double = a + b

  override def finalResult(t: Traversable[Any], result: Double): Double = result
}
