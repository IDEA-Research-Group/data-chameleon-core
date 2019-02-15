package es.us.idea.adt.data.chameleon.spark

import es.us.idea.adt.data.chameleon.{CreateStruct, CreateAttribute}
import es.us.idea.adt.data.chameleon.dsl.CreateAttributeContainer
import es.us.idea.adt.data.chameleon.spark.conversor.{SparkDataConversor, SparkTypeConversor}
import org.apache.spark.sql.functions.{array, explode, struct, udf, col}
import org.apache.spark.sql.{DataFrame, Row}

/** Singleton object with implicit functions for Apache Spark
  *
  * */
object implicits {

  /** Applies the transformations specified as a parameter in the adt method over this DataFrame
    *
    * */
  implicit class ChameleonImplicit(df: DataFrame) {
    def chameleon(rc: CreateAttributeContainer*): DataFrame = {
      transformDataFrame(df, rc.map(t => t.build() match {
        case r: CreateAttribute => r
        case _ => throw new Exception("chameleon method must receive rename evaluable objects")
      }): _*)
    }
  }

  private def transformDataFrame(df: DataFrame, dt: CreateAttribute*): DataFrame = {
    // The temporal column name is identified with the hexadecimal timestamp
    val tempColumn = s"__${System.currentTimeMillis().toHexString}"

    // Generates the DataStructure object, adding the specified transformations to apply
    val transformations = new CreateStruct(dt)

    // Transform Spark Schema to Chameleon Schema
    val chameleonSchema = SparkTypeConversor.spark2Chameleon(df.schema)

    // Generates the resulting schema
    val chameleonResultSchema =
      transformations.evaluate(chameleonSchema) /*match {
        case adtDataType: ADTDataType => adtDataType.get
        case _ => throw new UnsupportedDataStructureException("The generated data structure is not valid.")
      }*/

    val sparkResultSchema = SparkTypeConversor.chameleon2Spark(chameleonResultSchema)

    // This UDF applies the transformation on a Row object, setting the previous calculated schema
    val dataMappingUdf = udf((row: Row) => {
      val chameleonData = SparkDataConversor.spark2chameleon(row)
      val transformed = transformations.applyTransformation(chameleonData)
      SparkDataConversor.chameleon2spark(transformed, chameleonResultSchema)
    },
      sparkResultSchema
    )

    // Collects the fields to be selected
    //val fToSelect = utils.findMinSetOfPaths(dt.flatMap(_.getFirstLevelPath())).map(col)
    val fToSelect = df.columns.map(col)

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
