package es.us.idea.adt.data.chameleon.internal.dtf.flatten

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.complex.ArrayType
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

class Flatten(evaluable: Evaluable) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    utils.unwrap(evaluable.getValue(utils.unwrap(in))) match {
      case seq: Seq[Any] =>
        Some(
          seq.foldLeft(Seq(): Seq[Any])((accum, el) => utils.unwrap(el) match {
            case s: Seq[Any] => accum ++ s
            case other => accum :+ other
          } )
        )
      case _ => None
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {

    evaluable.evaluate(parentDataType) match {
      case arrayType: ArrayType => arrayType.getElementDataType match {
        case nestedArrayType: ArrayType => new ArrayType(nestedArrayType.getElementDataType)
        case other => throw new Exception(s"Flatten DTF requires ArrayType(ArrayType(...)). Instead, it was $other")
      }
      case other => throw new Exception(s"Flatten DTF requires ArrayType(ArrayType(...)). Instead, it was $other")
    }

  }
}
