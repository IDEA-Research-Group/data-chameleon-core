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
            val aggregationResult =
              t.foldLeft[Option[Double]](Some(getInitialValue))((acc, element) => {
                //val ensuredElement = utils.ensureDataType(element, getDataType)
                if(acc != None) {
                  val a = utils.ensureNumber(acc)
                  val b = utils.ensureNumber(element)
                  a match {
                    case Some(n: Double) => b match {
                      case Some(m: Double) => Some(operation(n, m))
                      case _ => None
                    }
                    case _ => None
                  }
                } else None
              })

            val result =
              aggregationResult match {
                case Some(d: Double) => finalResult(t, d)
                case _ => None
              }

            utils.ensureDataType(result, getDataType)

          }
          case _ => None
        }
      case _ => None
    }
  }

  def getInitialValue: Double
  def operation(a: Double, b: Double): Double
  def finalResult(t: Traversable[Any], result: Double): Double

}
