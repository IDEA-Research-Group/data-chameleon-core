package es.us.idea.adt.dsl

import es.us.idea.adt.data.functions.ADTReductionFunction
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
    def *(iterableFieldContainer: IterableFieldContainer) = new NamedFieldContainer(name, iterableFieldContainer)
    def +(namedFieldContainer: NamedFieldContainer*) = new NamedFieldContainer(name, new DataStructureContainer(namedFieldContainer: _*))
  }

  def max(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reductionFunctions.max)
  def max(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reductionFunctions.max)

  def min(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reductionFunctions.min)
  def min(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reductionFunctions.min)

  def avg(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reductionFunctions.avg)
  def avg(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reductionFunctions.avg)

  def sum(container: Container*): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(functions.reductionFunctions.sum)
  def sum(iterableFieldContainer: IterableFieldContainer): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(functions.reductionFunctions.sum)

  def reduce(container: Container*)(adtReductionFunction: ADTReductionFunction): StructureModifierContainer = new StructureModifierContainer(new DataSequenceContainer(container: _*))(adtReductionFunction)
  def reduce(iterableFieldContainer: IterableFieldContainer)(adtReductionFunction: ADTReductionFunction): StructureModifierContainer = new StructureModifierContainer(iterableFieldContainer)(adtReductionFunction)

}
