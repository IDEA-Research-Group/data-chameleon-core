package es.us.idea.adt.data.chameleon.dsl
import es.us.idea.adt.data.chameleon.CreateArray
import es.us.idea.adt.data.chameleon.internal.Evaluable

class ArrayContainer(expressionContainers: Seq[ExpressionContainer]) extends ExpressionContainer {
  override def build(): Evaluable = new CreateArray(expressionContainers.map(_.build()))
}
