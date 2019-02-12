package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class BooleanType extends SimpleType{

  type InternalType = Boolean

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

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
