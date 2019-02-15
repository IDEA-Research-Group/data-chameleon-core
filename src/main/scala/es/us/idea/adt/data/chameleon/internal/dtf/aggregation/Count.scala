package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.data.simple.IntegerType
import es.us.idea.adt.data.chameleon.data.{DataType}
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class Count(eval: Evaluable) extends DTFOperator {

  override var dataType: Option[DataType] = None

  def getValue(in: Any): Any = {

    val value = eval.getValue(in)

    utils.unwrap(value) match {
      case t: Traversable[Any] => t.size
      case _ => None
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new IntegerType
    this.dataType = Some(dt)
    dt
  }
}
