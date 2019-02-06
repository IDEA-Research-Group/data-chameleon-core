package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

class DoubleType extends SimpleType {
  override def toString: String = "Double"

  override def equals(that: Any): Boolean = {
    that match {
      case that: DoubleType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

}
