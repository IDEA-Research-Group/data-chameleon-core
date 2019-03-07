package es.us.idea.adt.data.chameleon.internal.dtf.filter

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates.Predicate

class Filter(eval: Evaluable, predicate: Predicate) extends DTFOperator {

  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    def matchValue: Any => Any = (a: Any) => {
      a match {
        case t: Traversable[_] => t.filter(element => predicate.eval(element))
        //case list: List[Any] => list.map(element => operation.getValue(element))
        //case seq: Seq[Any] => seq.map(element => operation.getValue(element))
        case Some(any) => matchValue(any)
        case _ => None //throw new Exception("Iterable must be applied on an array")
      }
    }

    val valueToIterate = eval.getValue(in)
    matchValue(valueToIterate)
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt =
      eval.evaluate(parentDataType) match {
        case at: ArrayType => at
        case other => throw new Exception(s"Filter DTF must be applied to ArrayType. Instead, it was applied to $other")
      }
    this.dataType = Some(dt)
    dt
  }

  override def toString(): String = s"Filter(${eval.toString}, ${predicate.toString})"

}
