package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

import es.us.idea.adt.data.chameleon.internal.Evaluable

class LessEqualThan(left: Evaluable, right: Any) extends NumericBinaryPredicate(left, right) {
  override def operation(left: Double, right: Double): Boolean = left <= right
}
