package es.us.idea.adt.dsl
import es.us.idea.adt.data.{Data, DataModifier}
import org.apache.spark.sql.types.DataType

class DataModifierContainer(container: Container with Modifiable)(f: (Any => Option[Any], Option[DataType])) extends Container with Modifiable {
  override def build(): Data = new DataModifier(container.build(), f)
}
