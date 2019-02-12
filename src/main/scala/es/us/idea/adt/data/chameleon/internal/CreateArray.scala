package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.{ArrayEvaluable, Evaluable}

class CreateArray(evals: Seq[Evaluable]) extends ArrayEvaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = evals.map(e => e.getValue(in) )

  override def evaluate(parentDataType: DataType): DataType = {
    val distinct = evals.map(_ evaluate parentDataType).distinct
    // TODO: Si está vacia, el tipo sería null if()
    val dt =
      if(distinct.size equals 1) new ArrayType(distinct.head)
      else throw new Exception("Array data types must be composed of elements with the same data type")
    this.dataType = Some(dt)
    dt
  }
}
