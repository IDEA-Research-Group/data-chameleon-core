package es.us.idea.adt.data.chameleon.dsl
import es.us.idea.adt.data.chameleon.Iterate
import es.us.idea.adt.data.chameleon.internal.Evaluable

class IterateContainer(toIterate: ExpressionContainer, operation: ExpressionContainer) extends ExpressionContainer {
  override def build(): Evaluable = new Iterate(toIterate.build(), operation.build())
}
