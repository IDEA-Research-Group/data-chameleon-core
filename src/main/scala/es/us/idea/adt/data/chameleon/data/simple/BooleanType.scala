package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

class BooleanType extends SimpleType{
  override def toString: String = "Boolean"

  override def equals(that: Any): Boolean = {
    that match {
      case that: BooleanType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }
}
