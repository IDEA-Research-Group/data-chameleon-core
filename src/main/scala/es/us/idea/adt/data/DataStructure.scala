package es.us.idea.adt.data

import es.us.idea.adt.data.exceptions.UnsupportedDataStructureException
import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes
import utils._

/** Represents a data structure, composed by at least a field. A field is expected to have a name and a Data.
  *
  * @param data the set of Data objects to be joint
  */
class DataStructure(data: Data*) extends DataUnion(data: _*) {

  /**
    *
    * @param in the source data structure where the value must be found
    * @return the extracted Data value
    */
  override def getValue(in: Any): Any = Row.apply(data.map(d => findAndReplaceMaps(d.getValue(in))  ): _*)

  /**
    *
    * @param schema the source schema
    * @return the resulting schema
    */
  override def getSchema(schema: ADTSchema): ADTSchema = {
    new ADTDataType(
      DataTypes.createStructType(
        data.zipWithIndex.map(t => t._1.getSchema(schema) match {
          case adtStructField: ADTStructField => adtStructField.get
          case adtDataType: ADTDataType => DataTypes.createStructField(t._2.toString, adtDataType.get, true)
          case _ => throw new UnsupportedDataStructureException(s"Unexpected schema structure for a DataStructure object: $schema")
        }).toArray
      )
    )
  }
}