package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}

class Rename(name: String, eval: Evaluable) extends Evaluable {

  def getName: String = name

  override def getValue(in: Any): Any = eval.getValue(in)

  override def getDataType(dataType: DataType): DataType = {
    new Attribute(name, eval.getDataType(dataType))
  }
}
