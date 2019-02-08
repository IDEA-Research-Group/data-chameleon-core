package es.us.idea.adt.data.chameleon.internal

import es.us.idea.adt.data.chameleon.data.DataType

class NAryOperator(evals: Seq[Evaluable], dtf: DTF) extends Evaluable {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    dtf.evaluate(evals.map(_.getValue(in)): _*)
  }

  override def evaluate(parentDataType: DataType): DataType = {
    //dtf.evaluate(dataType)
    // TODO: Por el momento, va a ser obligatorio indicar el datatype en la función
    val dt = dtf.getDataType()
    this.dataType = Some(dt)
    dt
  }
}
