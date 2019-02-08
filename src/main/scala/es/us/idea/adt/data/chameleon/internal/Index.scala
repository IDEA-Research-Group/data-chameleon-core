package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.Evaluable

class Index(index: Int) extends Evaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    in match {
      case arr: Array[Any] => arr.lift(index)
      case list: List[Any] => list.lift(index)
      case seq: Seq[Any] => seq.lift(index)
      case Some(a) => getValue(a)
      case _ => None
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      parentDataType match {
        case array: ArrayType => array.getElementDataType
        case _ => throw new Exception("Select operator must be applied on an Array data type")
      }
    this.dataType = Some(dt)
    dt
  }

}
