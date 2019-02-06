package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

class DateType extends SimpleType{
  override def toString: String = "Date"

  override def equals(that: Any): Boolean = {
    that match {
      case that: DateType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

}
