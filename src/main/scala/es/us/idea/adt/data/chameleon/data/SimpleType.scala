package es.us.idea.adt.data.chameleon.data

abstract class SimpleType extends DataType {
  type InternalType

  def getOrdering: Ordering[InternalType]
}
