package es.us.idea.adt.data.chameleon.data.simple

import java.util.Date

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class DateType extends SimpleType{

  type InternalType = Date

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

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
