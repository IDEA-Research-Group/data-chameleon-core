package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

class First(eval: Evaluable) extends AbstractAggregationDTF(eval) {
  override def getValue(in: Any): Any = {
    val value = eval.getValue(in)
    utils.unwrap(value) match {
      case t: Traversable[Any] => t.head
      case _ => None
    }
  }
}
