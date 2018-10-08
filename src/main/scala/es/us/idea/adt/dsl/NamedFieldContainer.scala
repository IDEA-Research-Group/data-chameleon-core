package es.us.idea.adt.dsl

import es.us.idea.adt.data.{Data, NamedField}

class NamedFieldContainer(name: String, container: Container) extends Container {
  def getName = name

  def &+(namedFieldContainer: NamedFieldContainer*): NamedFieldContainer = {
    container match {
      case basicFieldContainer: BasicFieldContainer => new NamedFieldContainer(name, new IterableFieldContainer(basicFieldContainer.getPath(), new DataStructureContainer(namedFieldContainer: _*)))
      case _ => throw new Exception("The interpreter needs a path to iterate over it")
    }
  }

  def &*(containers: Container*): NamedFieldContainer = {
    container match {
      case basicFieldContainer: BasicFieldContainer => new NamedFieldContainer(name, new IterableFieldContainer(basicFieldContainer.getPath(), new DataSequenceContainer(containers: _*)))
      case _ => throw new Exception("The interpreter needs a path to iterate over it")
    }
  }

  def &(nestedContainer: Container): NamedFieldContainer = {
    container match {
      case basicFieldContainer: BasicFieldContainer => new NamedFieldContainer(name, new IterableFieldContainer(basicFieldContainer.getPath(), nestedContainer))
      case _ => throw new Exception("The interpreter needs a path to iterate over it")
    }
  }

  override def build(): Data = new NamedField(name, container.build())
}