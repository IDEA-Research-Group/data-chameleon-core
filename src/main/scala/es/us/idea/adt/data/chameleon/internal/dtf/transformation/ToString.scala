package es.us.idea.adt.data.chameleon.internal.dtf.transformation

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple.StringType
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class ToString(eval: Evaluable) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = utils.TypeConversions.asString(eval.getValue(in))

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new StringType()
    this.dataType = Some(dt)
    dt
  }
}
