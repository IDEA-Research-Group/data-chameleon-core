package es.us.idea.adt.data.chameleon.data.complex

import es.us.idea.adt.data.chameleon.data.{Attribute, ComplexType}

class StructType(attributes: Attribute*) extends ComplexType{

  def getAttributes: Seq[Attribute] = attributes

  // TODO: Si el atributo no se ha encontrado, lanza una excepción. Esto se podría mejorar
  def findAttribute(name: String): Attribute = {
    attributes.find(a => a.getName == name) match {
      case Some(a) => a
      case _ => throw new Exception(s"Attribute $name is not in the source schema")
    }
  }

  override def toString: String = s"Struct(${attributes.map(a => a.toString).mkString(", ")})"

}
