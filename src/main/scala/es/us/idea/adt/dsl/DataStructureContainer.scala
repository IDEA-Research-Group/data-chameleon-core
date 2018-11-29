package es.us.idea.adt.dsl

import es.us.idea.adt.data.{Data, DataStructure}

class DataStructureContainer(container: Container*) extends DataUnionContainer(container: _*) {
  override def build(): Data = new DataStructure(container.map(_.build()): _*)
}
