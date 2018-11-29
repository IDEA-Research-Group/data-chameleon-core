package es.us.idea.adt.data

import es.us.idea.adt.data.exceptions.UnsupportedDataStructureException
import es.us.idea.adt.data.schema.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.types.DataTypes

/** Represents a Data sequence. The getValue will return a collection with the Data specified in the data parameter.
  *
  * @param data the set of Data objects to be joint
  */
class DataSequence(data: Data*) extends DataUnion(data: _*) {

  /**
    *
    * @param schema the source schema
    * @return the resulting schema
    */
  override def getSchema(schema: ADTSchema): ADTSchema = {

    // Todos los datos deben ser del mismo tipo
    val d = data.map(_.getSchema(schema)).distinct

    if (d.isEmpty) new ADTDataType(DataTypes.createArrayType(DataTypes.NullType))
    else if (d.size equals 1)
      d.head match {
        case adtDataType: ADTDataType => new ADTDataType(DataTypes.createArrayType(adtDataType.get))
        case adtStructField: ADTStructField => new ADTDataType(DataTypes.createArrayType(adtStructField.get.dataType))
        case _ => throw new Exception()
      }
    // else new ADTDataType(DataTypes.StringType)
    // Temporary it will throw an exception if the DataType are distinct
    else throw new UnsupportedDataStructureException("The Data sequences cannot have different data types")
  }

}