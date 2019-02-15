package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

class Or(predicate: Predicate*) extends Predicate {
  override def eval(in: Any): Boolean = predicate.exists(_.eval(in))
}
