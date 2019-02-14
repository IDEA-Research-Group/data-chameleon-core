package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.data.DataType

trait Evaluable extends Serializable {

  var dataType: Option[DataType]

  def getValue(in: Any): Any

  def applyTransformation(in: Any) = {
    if(!hasBeenEvaluated) throw new Exception("The expression has not been evaluated yet.")
    getValue(in)
  }

  def evaluate(parentDataType: DataType): DataType

  def hasBeenEvaluated: Boolean = dataType.isDefined

  def getDataType: DataType = {
    dataType match {
      case Some(x) => x
      case _ => throw new Exception("The expression has not been evaluated yet.")
    }
  }

}
