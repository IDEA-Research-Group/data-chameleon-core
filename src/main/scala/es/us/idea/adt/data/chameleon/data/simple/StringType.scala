package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class StringType extends SimpleType {

  type InternalType = String

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

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
