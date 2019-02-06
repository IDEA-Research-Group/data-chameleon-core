package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.{DataType}

trait Evaluable {
  def getValue(in: Any): Any

  def getDataType(dataType: DataType): DataType
}
