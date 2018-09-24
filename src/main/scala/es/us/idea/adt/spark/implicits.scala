package es.us.idea.adt.spark

import es.us.idea.adt.data.exceptions.UnsupportedDataStructureException
import es.us.idea.adt.data.{Data, DataStructure, NamedField}
import es.us.idea.adt.data.schema.{ADTDataType, ADTStructField}
import es.us.idea.adt.spark.utils.fromRowToMap
import org.apache.spark.sql.functions.{array, explode, struct, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

/** Singleton object with implicit functions for Apache Spark
  *
  * */
object implicits {

  /** Applies the transformations specified as a parameter in the adt method over this DataFrame
    *
    * */
  implicit class DFADT(df: DataFrame) {

    def adt(dt: NamedField*): DataFrame = {
      // The temporal column name is identified with the hexadecimal timestamp
      val tempColumn = s"__${System.currentTimeMillis().toHexString}"

      // Generates the DataStructure object, adding the specified transformations to apply
      val transformations = new DataStructure(dt: _*)

      // Generates the resulting schema
      val schema =
        transformations.getSchema(new ADTDataType(df.schema)) match {
          case adtDataType: ADTDataType => adtDataType.get
          case _ => throw new UnsupportedDataStructureException("The generated data structure is not valid.")
        }

      // This UDF applies the transformation on a Row object, setting the previous calculated schema
      val dataMappingUdf = udf((row: Row) => {
        transformations.getValue(fromRowToMap(row))
      },
        schema
      )

      // Collects the fields to be selected
      val fToSelect = utils.findMinSetOfPaths(dt.flatMap(_.getFirstLevelPath())).map(col)

      // Applies the UDF to the DataFrame.
      // explode(array(..)) is a workaround to prevent Spark from executing many times the UDF
      // Further information: https://issues.apache.org/jira/browse/SPARK-17728
      val transformedDF = df.withColumn(tempColumn, explode(array(dataMappingUdf(struct(fToSelect: _*)))))

      // This list matches the actual column names with their final form
      val names = dt.map(d => (s"$tempColumn.${d.getName}", d.getName))

      // Renames all the generated columns during the transformation
      names.foldLeft(transformedDF)((acc, n) => acc.withColumn(n._2, col(n._1))).drop(col(tempColumn))
    }
  }
}
