package es.us.idea.adt.data.chameleon.dsl
import es.us.idea.adt.data.chameleon.CreateAttribute
import es.us.idea.adt.data.chameleon.internal.Evaluable

class CreateAttributeContainer(name: String, expressionContainer: ExpressionContainer) extends ExpressionContainer {
  override def build(): Evaluable = new CreateAttribute(name, expressionContainer.build())
}
