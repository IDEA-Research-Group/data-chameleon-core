package es.us.idea.adt.data.chameleon.dsl

import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates._

abstract class ExpressionContainer {
  def iterate(expressionContainer: ExpressionContainer) = new IterateContainer(this, expressionContainer)

  def filter(predicate: Predicate): FilterContainer = new FilterContainer(this, predicate)

  def ->(f: (Evaluable => DTFOperator)) = f.apply(this.build())

  // If this operator receives a String, it will rename the current expression
  //def ~(newName: String) = new Rename(newName, this.build())

  def <(value: Any) = new LessThan(this.build(), value)
  def <=(value: Any) = new LessEqualThan(this.build(), value)
  def >(value: Any) = new GreaterThan(this.build(), value)
  def >=(value: Any) = new GreaterEqualThan(this.build(), value)
  def ===(value: Any) = new Equal(this.build(), value)
  def !==(value: Any) = new NotEqual(this.build(), value)

  def build(): Evaluable
}
