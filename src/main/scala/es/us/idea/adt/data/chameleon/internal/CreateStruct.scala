package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}
import es.us.idea.adt.data.chameleon.data.complex.StructType
import es.us.idea.adt.data.chameleon.internal.Evaluable

class CreateStruct(attrs: Seq[Rename]) extends Evaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = attrs.map(r => (r.getName, r.getValue(in)) ).toMap

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      new StructType(attrs.map(r => r.evaluate(parentDataType) match {
          case a: Attribute => a
          case _ => throw new Exception("Struct data types must be composed of Attribute data types")
        }
      ): _*)
    this.dataType = Some(dt)
    dt
  }
}
