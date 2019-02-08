package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.Evaluable

class Iterate(toIterate: Evaluable, operation: Evaluable) extends Evaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    def matchValue: Any => Any = (a: Any) => {
      a match {
        case arr: Array[_] => arr.map(element => operation.getValue(element))
        case list: List[Any] => list.map(element => operation.getValue(element))
        case seq: Seq[Any] => seq.map(element => operation.getValue(element))
        case Some(any) => matchValue(any)
        case _ => None //throw new Exception("Iterable must be applied on an array")
      }
    }

    val valueToIterate = toIterate.getValue(in)
    matchValue(valueToIterate)
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      toIterate.evaluate(parentDataType) match {
        case at: ArrayType => new ArrayType(operation.evaluate(at.getElementDataType))
        case _ => throw new Exception("Iterable must be applied on an array")
      }
    this.dataType = Some(dt)
    dt
  }

}
