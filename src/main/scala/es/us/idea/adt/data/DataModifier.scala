package es.us.idea.adt.data

import es.us.idea.adt.data.functions.ADTDataFunction
import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema}

class DataModifier(data: Data, f: ADTDataFunction) extends Data {

  override def getValue(in: Any): Any = f.eval(data.getValue(in))

  override def getSchema(schm: ADTSchema): ADTSchema = {
    f.dataType match {
      case None => data.getSchema(schm)
      case Some(x) => new ADTDataType(x)
    }
  }

  override def getFirstLevelPath(): Seq[String] = data.getFirstLevelPath()

}