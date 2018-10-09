package es.us.idea.adt.data.functions

import org.apache.spark.sql.types.DataType

case class ADTDataFunction(f: AnyRef, dataType: Option[DataType]) {

  def eval(value: Any): Any = {

    val getRawValue = (va: Any) => va match {
      case Some(a) => a
      case other => other
    }

    f match {
      case a: (() => Any) => a()
      case b: (Any => Any) => b(getRawValue(value))
    }
  }

}

object ADTDataFunction {
  def apply(f: AnyRef, dataType: DataType): ADTDataFunction = new ADTDataFunction(f, Some(dataType))
}