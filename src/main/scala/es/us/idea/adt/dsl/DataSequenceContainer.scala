package es.us.idea.adt.dsl

import es.us.idea.adt.data.{Data, DataSequence}

class DataSequenceContainer(container: Container*) extends DataUnionContainer(container: _*) {
  override def build(): Data = new DataSequence(container.map(_.build()): _*)
}