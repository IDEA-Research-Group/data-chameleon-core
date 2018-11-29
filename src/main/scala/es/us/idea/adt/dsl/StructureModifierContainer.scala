package es.us.idea.adt.dsl

import es.us.idea.adt.data.functions.ADTReductionFunction
import es.us.idea.adt.data.{Data, DataUnion, StructureModifier}

class StructureModifierContainer(dataUnionContainer: DataUnionContainer)(f: ADTReductionFunction) extends Container with Modifiable {
  override def build(): Data = {
    dataUnionContainer.build() match {
      case du: DataUnion => new StructureModifier(du, f)
      case _ => throw new Exception("The structure modifier didn't receive a DataUnion") // TODO crear exception personalidada como "parse error exception o algo asi"
    }

  }
}