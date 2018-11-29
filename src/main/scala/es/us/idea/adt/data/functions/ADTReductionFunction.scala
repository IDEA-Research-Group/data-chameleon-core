package es.us.idea.adt.data.functions

import org.apache.spark.sql.types.DataType

case class ADTReductionFunction(f: AnyRef, dataType: DataType) {
  def eval(value: Seq[Any]): Any = {
    f match {
      case fseq: (Seq[Any] => Any) => fseq(value)
      case _ => throw new Exception("ADTReductionFunction must receive a function with a Sequence object") // TODO create custom exception
    }
  }
}

