package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

class And(predicate: Predicate*) extends Predicate {
  //override def eval(in: Any): Boolean = predicate.map(_.eval(in)).foldLeft(true)(_ && _)
  override def eval(in: Any): Boolean = predicate.forall(_.eval(in))
}
