package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.internal.Evaluable

class Max(eval: Evaluable) extends ComparisonAggregationDTF(eval) {
  override def compare(element1: Any, element2: Any, ordering: Ordering[Any]): Boolean = ordering.gt(element1, element2)
}
