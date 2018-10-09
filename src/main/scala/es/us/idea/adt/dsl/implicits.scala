package es.us.idea.adt.dsl

import es.us.idea.adt.functions

object implicits {

  implicit class StringToDestinationContainer(val sc: StringContext) {
    def d(args: Any*): DestinationContainer = {
      DestinationContainer(sc.s(args: _*))
    }
  }

  def d(str: String) = DestinationContainer(str)

  implicit def strToContainer(str: String): BasicFieldContainer = new BasicFieldContainer(str)

  case class DestinationContainer(name: String) {
    def <(container: Container): NamedFieldContainer = new NamedFieldContainer(name, container)
    def *(container: Container*) = new NamedFieldContainer(name, new DataSequenceContainer(container: _*))
    def +(namedFieldContainer: NamedFieldContainer*) = new NamedFieldContainer(name, new DataStructureContainer(namedFieldContainer: _*))
  }

  def max(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reduce.max)
  def max(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reduce.max)

  def min(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reduce.min)
  def min(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reduce.min)

  def avg(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reduce.avg)
  def avg(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reduce.avg)

  def sum(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reduce.sum)
  def sum(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reduce.sum)

}
