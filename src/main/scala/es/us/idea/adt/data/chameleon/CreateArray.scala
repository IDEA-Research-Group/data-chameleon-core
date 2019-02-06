package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType

class CreateArray(evals: Seq[Evaluable]) extends Evaluable {
  //def this(evals: Evaluable*) {
  //  this(evals: _*)
  //}

  override def getValue(in: Any): Any = evals.map(e => e.getValue(in) )

  override def getDataType(dataType: DataType): DataType = {
    val distinct = evals.map(_ getDataType dataType).distinct
    println(distinct)
    // TODO: Si está vacia, el tipo sería null if()
    if(distinct.size equals 1) new ArrayType(distinct.head)
    else throw new Exception("Array data types must be composed of elements with the same data type")
  }
}
