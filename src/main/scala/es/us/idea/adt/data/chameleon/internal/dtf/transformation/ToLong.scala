package es.us.idea.adt.data.chameleon.internal.dtf.transformation

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple.LongType
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class ToLong(eval: Evaluable) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = utils.TypeConversions.asLong(eval.getValue(in))

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new LongType()
    this.dataType = Some(dt)
    dt
  }
}
