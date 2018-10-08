package es.us.idea.adt.dsl

import es.us.idea.adt.data.{Data, IterableField}

class IterableFieldContainer(path: String, container: Container) extends DataUnionContainer {
  override def build(): Data = new IterableField(path, container.build())
}
