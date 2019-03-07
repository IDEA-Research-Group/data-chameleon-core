package es.us.idea.adt.data.chameleon.internal.dtf.transformation

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple.FloatType
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class ToFloat(eval: Evaluable) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = utils.TypeConversions.asFloat(eval.getValue(in))

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new FloatType()
    this.dataType = Some(dt)
    dt
  }

  override def toString(): String = s"ToFloat(${eval.toString})"

}
