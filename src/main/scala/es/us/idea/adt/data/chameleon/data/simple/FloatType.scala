package es.us.idea.adt.data.chameleon.data.simple

import es.us.idea.adt.data.chameleon.data.SimpleType

import scala.math.Ordering

class FloatType extends SimpleType {

  type InternalType = Float

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

  override def getOrdering: Ordering[InternalType] = implicitly[Ordering[InternalType]]

}
