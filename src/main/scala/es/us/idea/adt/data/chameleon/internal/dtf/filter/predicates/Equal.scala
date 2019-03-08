package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

import es.us.idea.adt.data.chameleon.internal.Evaluable

class Equal(left: Evaluable, right: Any) extends BinaryPredicate(left, right) {
  override def operation(left: Any, right: Any): Boolean = left == right
}
