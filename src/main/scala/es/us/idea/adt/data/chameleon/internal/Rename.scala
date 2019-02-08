package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}
import es.us.idea.adt.data.chameleon.internal.Evaluable

class Rename(name: String, eval: Evaluable) extends Evaluable {

  override var dataType: Option[DataType] = None

  def getName: String = name

  override def getValue(in: Any): Any = eval.getValue(in)

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new Attribute(name, eval.evaluate(parentDataType))
    this.dataType = Some(dt)
    dt
  }
}
