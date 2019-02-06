package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

class FloatType extends SimpleType {
  override def toString: String = "Float"

  override def equals(that: Any): Boolean = {
    that match {
      case that: FloatType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

}
