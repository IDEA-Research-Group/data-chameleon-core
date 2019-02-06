package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType

class IndexNested(index: Int, eval: Evaluable) extends Evaluable {

  override def getValue(in: Any): Any = {
    in match {
      case arr: Array[Any] => eval.getValue(arr.lift(index))
      case list: List[Any] => eval.getValue(list.lift(index))
      case seq: Seq[Any] => eval.getValue(seq.lift(index))
      case Some(a) => getValue(a)
      case _ => None
    }
  }

  override def getDataType(dataType: DataType): DataType = {
    dataType match {
      case array: ArrayType => eval.getDataType(array.getElementDataType)
      case _ => throw new Exception("Select operator must be applied on an Array data type")
    }
  }
}
