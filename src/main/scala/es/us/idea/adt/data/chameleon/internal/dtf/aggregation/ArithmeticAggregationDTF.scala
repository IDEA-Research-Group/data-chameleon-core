package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.data.SimpleType
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

// TODO no es tolerante a valores nulos. Si se encuentra con un valor nulo, devolverá nulo
abstract class ArithmeticAggregationDTF(eval: Evaluable) extends AbstractAggregationDTF(eval) {
  override def getValue(in: Any): Any = {
    val value = eval.getValue(in)
    utils.unwrap(value) match {
      case t: Traversable[Any] =>
        getDataType match {
          case st: SimpleType => {

            val result =
              t.foldLeft[Any](getInitialValue)((acc, element) => {
                val ensuredElement = utils.ensureDataType(element, getDataType)
                if(ensuredElement == None || acc == None) None
                else operation(acc, ensuredElement)
              })

            finalResult(t, result)

          }
          case _ => None
        }
      case _ => None
    }
  }

  def getInitialValue: Any
  def operation(a: Any, b: Any): Any
  def finalResult(t: Traversable[Any], result: Any): Any

}
