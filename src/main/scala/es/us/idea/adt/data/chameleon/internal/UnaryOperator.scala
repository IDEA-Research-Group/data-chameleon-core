package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.Operator
import es.us.idea.adt.data.chameleon.data.DataType

class UnaryOperator(eval: Evaluable, dtf: DTF) extends Operator(dtf) {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    dtf.evaluate(eval.getValue(in))
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = dtf.getDataType(eval.evaluate(parentDataType))
    this.dataType = Some(dt)
    dt
  }
}
