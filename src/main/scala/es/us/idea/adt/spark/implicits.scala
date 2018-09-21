package es.us.idea.adt.spark

import es.us.idea.adt.data.{Data, DataStructure, NamedField}
import es.us.idea.adt.data.schema.{ADTDataType, ADTStructField}
import es.us.idea.adt.spark.utils.fromRowToMap
import org.apache.spark.sql.functions.{array, explode, struct, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

object implicits {

  implicit class DFADT(df: DataFrame) {
    def adt(dt: NamedField*): DataFrame = {
      // The temporal column name is identified with the hexadecimal timestamp
      val tempColumn = s"__${System.currentTimeMillis().toHexString}"

      val transformations = new DataStructure(dt: _*)

      val schema = transformations.getSchema(new ADTDataType(df.schema)) match {
        case adtDataType: ADTDataType => adtDataType.get
        //case adtStructField: ADTStructField => DataTypes.createStructType(Array(adtStructField.get))
        case _ => throw new Exception("Unexpected data structure")
      }

      val dataMappingUdf = udf((row: Row) => {
        transformations.getValue(fromRowToMap(row))
      },
        schema
      )

      val df2 = df.withColumn(tempColumn, explode(array(dataMappingUdf(struct(df.columns.map(df.col(_)): _*)))))

      val names = dt.map(d => (s"$tempColumn.${d.getName}", d.getName))

      names.foldLeft(df2)((acc, n) => acc.withColumn(n._2, col(n._1))).drop(col(tempColumn))
    }
  }

}
