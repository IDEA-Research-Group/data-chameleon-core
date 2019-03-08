package es.us.idea.adt.data.chameleon.dsl

import es.us.idea.adt.data.chameleon.{CreateStruct, CreateAttribute}
import es.us.idea.adt.data.chameleon.internal.Evaluable

class StructContainer(renameContainers: Seq[CreateAttributeContainer]) extends ExpressionContainer {
  override def build(): Evaluable = {
    new CreateStruct(renameContainers.map(rc => {
      rc.build() match {
        case rename: CreateAttribute => rename
        case _ => throw new Exception("Exception when interpreting the DSL. Structs must be composed of renamed attributes")
      }
    }))
  }
}
