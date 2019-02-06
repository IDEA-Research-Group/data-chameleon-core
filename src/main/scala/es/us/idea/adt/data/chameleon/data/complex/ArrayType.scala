package es.us.idea.adt.data.chameleon.data.complex

import es.us.idea.adt.data.chameleon.data.{ComplexType, DataType}

class ArrayType(elementDataType: DataType) extends ComplexType {

  def getElementDataType: DataType = elementDataType

  override def toString: String = s"Array(${elementDataType.toString})"
}
