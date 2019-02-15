package es.us.idea.adt.data.chameleon.dsl
import es.us.idea.adt.data.chameleon.Rename
import es.us.idea.adt.data.chameleon.internal.Evaluable

class RenameContainer(name: String, expressionContainer: ExpressionContainer) extends ExpressionContainer {
  override def build(): Evaluable = new Rename(name, expressionContainer.build())
}
