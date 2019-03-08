package es.us.idea.adt.data.chameleon.data

class Attribute(name: String, dataType: DataType) extends DataType {

  def getName: String = name

  def getDataType: DataType = dataType

  override def toString: String = s"Attribute($name, ${dataType.toString})"

}
