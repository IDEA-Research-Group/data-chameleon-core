package es.us.idea.adt.dsl
import es.us.idea.adt.data.functions.ADTDataFunction
import es.us.idea.adt.data.{Data, DataModifier}

class DataModifierContainer(container: Container with Modifiable)(f: ADTDataFunction) extends Container with Modifiable {
  override def build(): Data = new DataModifier(container.build(), f)
}
