package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

class StringType extends SimpleType {
  override def toString: String = "String"

  override def equals(that: Any): Boolean = {
    that match {
      case that: StringType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

}
