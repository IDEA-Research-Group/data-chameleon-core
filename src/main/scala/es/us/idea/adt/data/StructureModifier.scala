package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

class StructureModifier(data: DataUnion, operation: (Seq[Any] => Any, DataType) ) extends Data {
  override def getValue(in: Any): Any = {

    val processIn = (a: Any) => a match {
      case seq: Seq[Any] => operation._1(seq)
      case row: Row => operation._1(row.toSeq)
      case x => None
    }

    data.getValue(in) match {
      case Some(x) => processIn(x)
      case other => processIn(other)
    }

  }

  override def getSchema(schm: ADTSchema): ADTSchema = {
    new ADTDataType(operation._2)
  }

  override def getFirstLevelPath(): Seq[String] = data.getFirstLevelPath()
}