package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.data.{DataType, SimpleType}
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

abstract class AbstractAggregationDTF(eval: Evaluable) extends DTFOperator{

  override var dataType: Option[DataType] = None

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      eval.evaluate(parentDataType) match {
        case at: ArrayType => {
          val et = at.getElementDataType
          et match {
            case _: SimpleType => et
            case other => other //throw new Exception(s"Aggregation DTF must be applied to ArrayType of SimpleType. Instead, it has been applied to $other")
          }
        }
        case _ => throw new Exception("Aggregation DTF must be applied to ArrayType")
      }
    this.dataType = Some(dt)
    dt
  }

}
