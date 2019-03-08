package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.Evaluable

class IndexNested(index: Int, eval: Evaluable) extends Evaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    in match {
      case seq: Seq[Any] => eval.getValue(seq.lift(index))
      case Some(a) => getValue(a)
      case _ => None
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      parentDataType match {
        case array: ArrayType => eval.evaluate(array.getElementDataType)
        case _ => throw new Exception("Select operator must be applied on an Array data type")
      }
    this.dataType = Some(dt)
    dt
  }

  override def toString(): String = s"IndexNested($index, ${eval.toString})"

}
