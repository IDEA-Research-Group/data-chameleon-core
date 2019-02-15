package es.us.idea.adt.data.chameleon.internal.dtf.filter.predicates

import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}

abstract class NumericBinaryPredicate(left: Evaluable, right: Any) extends BinaryPredicate(left, right) {
  override def operation(left: Any, right: Any): Boolean = {
    val lOpt = utils.ensureNumber(left)
    val rOpt = utils.ensureNumber(right)

    lOpt match {
      case Some(lDouble: Double) =>
        rOpt match {
          case Some(rDouble: Double) => operation(lDouble, rDouble)
          case _ => false
        }
      case _ => false
    }

  }

  def operation(left: Double, right: Double): Boolean
}
