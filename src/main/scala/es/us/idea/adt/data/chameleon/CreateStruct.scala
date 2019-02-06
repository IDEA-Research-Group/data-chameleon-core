package es.us.idea.adt.data.chameleon
import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}
import es.us.idea.adt.data.chameleon.data.complex.StructType

class CreateStruct(attrs: Seq[Rename]) extends Evaluable {
  //def this(attrs: Rename*) {
  //  this(attrs: _*)
  //}

  override def getValue(in: Any): Any = attrs.map(r => (r.getName, r.getValue(in)) ).toMap

  override def getDataType(dataType: DataType): DataType = {
    new StructType(attrs.map(r => r.getDataType(dataType) match {
        case a: Attribute => a
        case _ => throw new Exception("Struct data types must be composed of Attribute data types")
      }
    ): _*)
  }
}
