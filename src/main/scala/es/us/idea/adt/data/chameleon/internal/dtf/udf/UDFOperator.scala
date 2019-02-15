package es.us.idea.adt.data.chameleon.internal.dtf.udf

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

class UDFOperator(evals: Seq[Evaluable], udf: UDF) extends DTFOperator{

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    udf.evaluate(evals.map(_.getValue(in)): _*)
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = udf.getDataType
    this.dataType = Some(dt)
    dt
  }
}
