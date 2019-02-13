package es.us.idea.adt.data.chameleon.dsl

import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.filter.Filter
import es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates.Predicate

class FilterContainer(expressionContainer: ExpressionContainer, predicate: Predicate) extends ExpressionContainer {
  override def build(): Evaluable = new Filter(expressionContainer.build(), predicate)
}
