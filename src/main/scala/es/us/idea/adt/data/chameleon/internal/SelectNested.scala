package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.StructType
import es.us.idea.adt.data.chameleon.internal.Evaluable

class SelectNested(name: String, eval: Evaluable) extends Evaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    in match {
      case m: Map[String, Any] => eval.getValue(m.get(name))
      case Some(a) => getValue(a)
      case _ => None
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
    parentDataType match {
        case struct: StructType => eval.evaluate(struct.findAttribute(name).getDataType)
        case _ => throw new Exception("Select operator must be applied on a Struct data type")
      }
    this.dataType = Some(dt)
    dt
  }

  override def toString(): String = s"SelectNested($name, ${eval.toString})"

}
