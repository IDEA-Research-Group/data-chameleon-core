package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.{Data, DataType}

trait Evaluable {
  def getValue(in: Data): Data

  def getDataType(dataType: DataType): DataType
}
