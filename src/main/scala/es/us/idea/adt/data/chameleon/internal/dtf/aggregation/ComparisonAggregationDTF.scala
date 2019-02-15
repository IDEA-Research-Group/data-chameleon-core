package es.us.idea.adt.data.chameleon.internal.dtf.aggregation

import es.us.idea.adt.data.chameleon.data.SimpleType
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

abstract class ComparisonAggregationDTF(eval: Evaluable) extends AbstractAggregationDTF(eval) {

  // TODO Implementar tratamiento de valores nulos. Actualmente, los valores nulos se eliminan antes de calcular máximos
  override def getValue(in: Any): Any = {
    val value = eval.getValue(in)
    utils.unwrap(value) match {
      case t: Traversable[Any] =>
        getDataType match {
          case st: SimpleType => {

            val ordering = st.getOrdering.asInstanceOf[Ordering[Any]]

            t.foldLeft[Any](None)((acc, element) => {
              val ensuredElement = utils.ensureDataType(element, getDataType)
              if((ensuredElement != null) && (ensuredElement != None)) {
                if(acc == None || compare(ensuredElement, acc, ordering)) ensuredElement else acc
              } else {
                acc
              }
            })
          }
          case _ => None
        }
      case _ => None
    }
  }

  def compare(element1: Any, element2: Any, ordering: Ordering[Any]): Boolean

}
