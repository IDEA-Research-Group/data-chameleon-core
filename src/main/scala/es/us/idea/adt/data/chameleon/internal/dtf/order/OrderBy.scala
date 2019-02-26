package es.us.idea.adt.data.chameleon.internal.dtf.order

import es.us.idea.adt.data.chameleon.data.{DataType, SimpleType}
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

class OrderBy(arrayevaluable: Evaluable, criteria: Evaluable, ascending: Boolean = true) extends DTFOperator{

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    val value = arrayevaluable.getValue(in)
    utils.unwrap(value) match {
      case t: Seq[Any] => {
        val criteriaDT = criteria.getDataType
        criteriaDT match {
          case simpleType: SimpleType => {
            val ordering = if(ascending) simpleType.getOrdering.asInstanceOf[Ordering[Any]] else simpleType.getOrdering.asInstanceOf[Ordering[Any]].reverse
            t.sortBy(e => utils.ensureDataType(criteria.getValue(e), simpleType))(ordering)
          }
          case _ => None
        }
      }
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      arrayevaluable.evaluate(parentDataType) match {
        case at: ArrayType => {
          // el tipo del criterio tiene que ser SIMPLE
          val criteriaDT = criteria.evaluate(at.getElementDataType)
          criteriaDT match {
            case simpleType: SimpleType => simpleType
            case other => throw new Exception(s"OderBy DTF criteria data type must be SimpleType. Instead, it was $other")
          }
          // Este DTF siempre devolverá un ArrayType de lo que sea
          at
        }
        //{
        //  val et = at.getElementDataType
        //  et match {
        //    case _: SimpleType => et
        //    case other => throw new Exception(s"Aggregation DTF must be applied to ArrayType of SimpleType. Instead, it has been applied to $other")
        //  }
        //}
        case _ => throw new Exception("OrderBy DTF must be applied to ArrayType")
      }
    this.dataType = Some(dt)
    dt
  }

  override def toString(): String = s"OrderBy(${arrayevaluable.toString}, ${criteria.toString}, $ascending)"

}
