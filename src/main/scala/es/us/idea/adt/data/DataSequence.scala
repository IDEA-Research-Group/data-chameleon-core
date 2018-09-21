package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.types.DataTypes

class DataSequence(data: Data*) extends DataUnion(data: _*) {
  //override def getValue(in: Any): Any = data.map(_.getValue(in))

  override def getSchema(schm: ADTSchema): ADTSchema = {
    // Todos los datos deben ser del mismo tipo

    val d = data.map(_.getSchema(schm)).distinct

    if(d.isEmpty) new ADTDataType(DataTypes.createArrayType(DataTypes.NullType))
    else if(d.size equals 1)
      d.head match {
        case adtDataType: ADTDataType => new ADTDataType(DataTypes.createArrayType(adtDataType.get))
        case adtStructField: ADTStructField => new ADTDataType(DataTypes.createArrayType(adtStructField.get.dataType))
        case _ => throw new Exception()
      }
    else new ADTDataType(DataTypes.StringType)

  }
}