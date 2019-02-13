package es.us.idea.adt.data.chameleon.dsl

import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

object implicits {

  /**
    * Implicit class to add the method << to String objects. It creates a RenameContainer
    * @param name
    */
  implicit class StringToAttributeNameContainer(val name: String) {
    def << (expressionContainer: ExpressionContainer) = new RenameContainer(name, expressionContainer)
  }

  /**
    * Implicit class to create a SelectContainer by means of a string sc preceded by the character 't'
    * @param sc
    */
  implicit class StringToSelectContainer(val sc: StringContext) {
    def t(args: Any*): SelectContainer = new SelectContainer(sc.s(args: _*))
  }

  def t(str: String) = new SelectContainer(str)

  def array(expressionContainers: ExpressionContainer*) = new ArrayContainer(expressionContainers)
  def struct(renameContainers: RenameContainer*) = new StructContainer(renameContainers)


  ///**
  //  * Implicitly converts a DTFOperator into an ExpressionContainer
  //  * @param dtfOperator
  //  */
  //implicit class DTFOperatorImplicit(dtfOperator: DTFOperator) extends ExpressionContainer {
  //  override def build(): Evaluable = dtfOperator
  //}

  /**
    * Implicitly converts a Evaluable into an ExpressionContainer
    * @param dtfOperator
    */
  implicit class EvaluableToExpressionContainer(evaluable: Evaluable) extends ExpressionContainer {
    override def build(): Evaluable = evaluable
  }

  /**
    * Implicitly converts an ExpressionContainer into an Evaluable object
    * @param expressionContainer
    * @return
    */
  implicit def expressionContainerToEvaluable(expressionContainer: ExpressionContainer): Evaluable = expressionContainer.build()

  //case class AttributeNameContainer(name: String) {
  //  def <= (expressionContainer: ExpressionContainer) = new RenameContainer(name, expressionContainer)
  //  //def <(container: Container): NamedFieldContainer = new NamedFieldContainer(name, container)
  //  //def *(container: Container*) = new NamedFieldContainer(name, new DataSequenceContainer(container: _*))
  //  //def *(iterableFieldContainer: IterableFieldContainer) = new NamedFieldContainer(name, iterableFieldContainer)
  //  //def +(namedFieldContainer: NamedFieldContainer*) = new NamedFieldContainer(name, new DataStructureContainer(namedFieldContainer: _*))
  //}
}
