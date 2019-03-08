package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.internal.Evaluable

class Min(eval: Evaluable) extends ComparisonAggregationDTF(eval) {
  override def compare(element1: Any, element2: Any, ordering: Ordering[Any]): Boolean = ordering.lt(element1, element2)
}
