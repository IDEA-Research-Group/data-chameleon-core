package es.us.idea.adt.data.chameleon.internal.dtf.transformation

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple.IntegerType
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

class ToInt(eval: Evaluable) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = utils.TypeConversions.asInt(eval.getValue( in))

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new IntegerType()
    this.dataType = Some(dt)
    dt
  }

  override def toString(): String = s"ToInt(${eval.toString})"

}
