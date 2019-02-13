package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple.DoubleType
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class Avg(eval: Evaluable) extends ArithmeticAggregationDTF(eval){

  override def getInitialValue: Double = 0.0

  override def operation(a: Double, b: Double): Double = a + b

  override def finalResult(t: Traversable[Any], result: Double): Double = result / t.count(utils.unwrap(_) != None)

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new DoubleType()
    this.dataType = Some(dt)
    dt
  }

}
