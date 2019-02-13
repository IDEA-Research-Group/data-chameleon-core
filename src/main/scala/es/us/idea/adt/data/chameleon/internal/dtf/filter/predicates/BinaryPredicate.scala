package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

// TODO: el operador right debería poder ser un elemento, ya sea anidado al que está siendo iterado u otro cualquiera del resto de la estructura.
// TODO para implementar esta funcionalidad, se requiere que in sea un grafo
abstract class BinaryPredicate(left: Evaluable, right: Any) extends Predicate {
  override def eval(in: Any): Boolean = {
    operation(utils.unwrap(left.getValue(in)), right)
  }

  def operation(left: Any, right: Any): Boolean

}
