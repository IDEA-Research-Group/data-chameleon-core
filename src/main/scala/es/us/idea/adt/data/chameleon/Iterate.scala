package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType

class Iterate(toIterate: Evaluable, operation: Evaluable) extends Evaluable {

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

  override def getDataType(dataType: DataType): DataType = {

    toIterate.getDataType(dataType) match {
      case at: ArrayType => new ArrayType(operation.getDataType(at.getElementDataType))
      case _ => throw new Exception("Iterable must be applied on an array")
    }
  }

}
