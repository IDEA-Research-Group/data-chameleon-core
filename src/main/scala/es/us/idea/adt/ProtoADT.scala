package es.us.idea.adt

import es.us.idea.adt.dsl.Composite2.{ADTDataType, ADTStructField, Data}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes


object ProtoADT {



  def protoADT(df: DataFrame, dt: Data): DataFrame = {

    val schema = dt.getSchema(new ADTDataType(df.schema)) match {
      case adtDataType: ADTDataType => adtDataType.get
      case adtStructField: ADTStructField => DataTypes.createStructType(Array(adtStructField.get))
    }


    val dataMappingUdf = udf((row: Row) => {
      val map = RowUtils.fromRowToMap(row)
      val r = dt.getValue(map)
      println(r)
      r
      //val a: Any = 1
      //Row.apply(a, 21, "aaasa", Seq(Seq(1.0, 2.0), Seq(-1.0, -2.0)))
      //mapping.Utils.buildRowFromMapAndDMSelector(map, dmSelector)

    }, //Utils.generateDataType(map)
      //DataTypes.createStructType(Array(
      //  DataTypes.createStructField("a", DataTypes.IntegerType, true ),
      //  DataTypes.createStructField("b", DataTypes.IntegerType, true ),
      //  DataTypes.createStructField("c", DataTypes.StringType, true ),
      //  DataTypes.createStructField("matrix", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)), true)
      //))
      schema
    )

    df.withColumn("adt_out", explode(array(dataMappingUdf(struct(df.columns.map(df.col(_)): _*)))))
    //df.withColumn("adt_out", dataMappingUdf(df.columns.map(df.col(_)): _*))

  }

}
