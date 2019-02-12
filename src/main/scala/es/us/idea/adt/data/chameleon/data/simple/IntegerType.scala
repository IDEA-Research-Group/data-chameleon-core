package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class IntegerType extends SimpleType{

  type InternalType = Integer

  override def toString: String = "Integer"

  override def equals(that: Any): Boolean = {
    that match {
      case that: IntegerType => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
