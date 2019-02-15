package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

trait Predicate {
  def eval(in: Any): Boolean
}
