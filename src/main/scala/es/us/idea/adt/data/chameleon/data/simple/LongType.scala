package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

class LongType extends SimpleType{
  override def toString: String = "Long"

  override def equals(that: Any): Boolean = {
    that match {
      case that: LongType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

}
