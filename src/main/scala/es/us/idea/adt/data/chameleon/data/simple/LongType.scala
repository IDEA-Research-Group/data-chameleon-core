package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class LongType extends SimpleType{

  type InternalType = Long

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

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
