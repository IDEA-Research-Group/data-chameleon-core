package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class Not(predicate: Predicate) extends Predicate {
  override def eval(in: Any): Boolean = !predicate.eval(in)
}
