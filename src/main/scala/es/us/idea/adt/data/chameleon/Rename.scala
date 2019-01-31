package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.{DataType, Attribute, Data}

class Rename(name: String, expr: Evaluable) extends Evaluable {
  override def getValue(in: Data): Data = expr.getValue(in)

  override def getDataType(dataType: DataType): DataType = new Attribute(name, expr.getDataType(dataType))
}
