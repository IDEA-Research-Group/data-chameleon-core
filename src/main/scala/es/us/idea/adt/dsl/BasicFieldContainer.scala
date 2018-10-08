package es.us.idea.adt.dsl

import es.us.idea.adt.data.{BasicField, Data}

class BasicFieldContainer(path: String) extends Container {
  def getPath() = path
  override def build(): Data = new BasicField(path)

  def &+(namedFieldContainer: NamedFieldContainer*): IterableFieldContainer = {
    new IterableFieldContainer(getPath(), new DataStructureContainer(namedFieldContainer: _*))
  }

  def &*(containers: Container*): IterableFieldContainer = {
    new IterableFieldContainer(getPath(), new DataSequenceContainer(containers: _*))
  }

  def &(nestedContainer: Container): IterableFieldContainer = {
    new IterableFieldContainer(getPath(), nestedContainer)
  }

}
