package es.us.idea.adt.dsl

import es.us.idea.adt.functions

object implicits {

  implicit class StringToDestinationContainer(val sc: StringContext) {
    def d(args: Any*): DestinationContainer = {
      DestinationContainer(sc.s(args: _*))
    }
  }

  implicit def strToContainer(str: String): BasicFieldContainer = new BasicFieldContainer(str)

  case class DestinationContainer(name: String) {
    def <(container: Container): NamedFieldContainer = new NamedFieldContainer(name, container)
    def *(container: Container*) = new NamedFieldContainer(name, new DataSequenceContainer(container: _*))
    def +(namedFieldContainer: NamedFieldContainer*) = new NamedFieldContainer(name, new DataStructureContainer(namedFieldContainer: _*))
  }

  def max(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.max)
  def max(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.max)

  def min(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.min)
  def min(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.min)

  def avg(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.avg)
  def avg(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.avg)

  def sum(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.sum)
  def sum(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.sum)

}
