package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.data.DataType

trait Evaluable {

  var dataType: Option[DataType]

  def getValue(in: Any): Any

  def evaluate(parentDataType: DataType): DataType

  def hasBeenEvaluated: Boolean = dataType.isDefined

  def getDataType: DataType = {
    dataType match {
      case Some(x) => x
      case _ => throw new Exception("The expression has not been evaluated yet.")
    }
  }

}
