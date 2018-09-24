package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.types.DataTypes

class NamedField(name: String, data: Data) extends Data {
  override def getValue(in: Any): Any = data.getValue(in)

  override def getSchema(schm: ADTSchema): ADTSchema = {
    data.getSchema(schm) match {
      case adtStructField: ADTStructField => new ADTStructField(DataTypes.createStructField(name, DataTypes.createStructType(Array(adtStructField.get)), true))
      case adtDataType: ADTDataType => new ADTStructField(DataTypes.createStructField(name, adtDataType.get, true))
      case _ => throw new Exception()
    }
  }
  def getName = name

  override def getFirstLevelPath(): Seq[String] = data.getFirstLevelPath()
}