package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class DoubleType extends SimpleType {

  type InternalType = Double

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

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
