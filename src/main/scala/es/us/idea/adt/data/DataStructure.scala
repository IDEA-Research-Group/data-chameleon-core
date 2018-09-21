package es.us.idea.adt.data

import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes
import utils._

class DataStructure(data: Data*) extends DataUnion(data: _*) {

  override def getValue(in: Any): Any = Row.apply(data.map(d => findAndReplaceMaps(d.getValue(in))  ): _*)

  override def getSchema(schm: ADTSchema): ADTSchema = {
    new ADTDataType(
      DataTypes.createStructType(
        data.zipWithIndex.map(t => t._1.getSchema(schm) match {
          case adtStructField: ADTStructField => adtStructField.get
          case adtDataType: ADTDataType => DataTypes.createStructField(t._2.toString, adtDataType.get, true)
          case _ => throw new Exception("Tipò de dato nmo reconocido")
        }).toArray
      )
    )
  }
}