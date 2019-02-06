package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.StructType

class SelectNested(name: String, eval: Evaluable) extends Evaluable {

  override def getValue(in: Any): Any = {
    in match {
      case m: Map[String, Any] => eval.getValue(m.get(name))
      case Some(a) => getValue(a)
      case _ => None
    }
  }

  override def getDataType(dataType: DataType): DataType = {
    //eval.getDataType(data)
    dataType match {
      case struct: StructType => eval.getDataType(struct.findAttribute(name).getDataType)
      case _ => throw new Exception("Select operator must be applied on a Struct data type")
    }
  }
}
