package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.StructType
import es.us.idea.adt.data.chameleon.internal.Evaluable

class Select(name: Option[String]) extends Evaluable{

  override var dataType: Option[DataType] = None


  def this(name: String) {
    this(Some(name))
  }

  def this() {
  this(None)
}

  override def getValue(in: Any): Any = {
    name match {
      case Some(s) => {
        in match {
          case m: Map[String, Any] => m.get(s)
          case Some(a) => getValue(a)
          case _ => None
        }
      }
      case _ => in
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = name match {
      case Some(s) => {
        parentDataType match {
          case struct: StructType => struct.findAttribute(s).getDataType
          case _ => throw new Exception("Select operator must be applied on a Struct data type")
        }
      }
      case _ => parentDataType
    }

    this.dataType = Some(dt)
    dt
  }

}
