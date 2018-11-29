package es.us.idea.adt.data

import es.us.idea.adt.data.functions.ADTReductionFunction
import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}
import org.apache.spark.sql.Row

class StructureModifier(data: DataUnion, f: ADTReductionFunction) extends Data {
  override def getValue(in: Any): Any = {

    val processIn = (a: Any) => a match {
      case seq: Seq[Any] => f.eval(seq)
      case row: Row => f.eval(row.toSeq)
      case x => None
    }

    data.getValue(in) match {
      case Some(x) => processIn(x)
      case other => processIn(other)
    }

  }

  override def getSchema(schm: ADTSchema): ADTSchema = {
    new ADTDataType(f.dataType)
  }

  override def getFirstLevelPath(): Seq[String] = data.getFirstLevelPath()
}