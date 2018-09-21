package es.us.idea.adt

import es.us.idea.adt.data._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import functions._

object Main {
  def main(args: Array[String]) = {

    //println("Hello World!")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark ADT")
      .getOrCreate()

    //import es.us.idea.adt.implicits.Helpers._
    //import es.us.idea.adt.dsl.DSL._

    import spark.implicits._




    val dataMappingUdf = udf((row: Row) => {
      val a: Any = "V"
      val b: Any = Option(0.0)

      val r: Any = Row.apply("ASDASDA", 9999)
      Row.apply(a, b, r, Seq(1.0.toString, 2.toString, "3.0", Row.apply("aa", 22).toString()))
    },
      DataTypes.createStructType(Array(
        DataTypes.createStructField("primera_prueba", DataTypes.StringType, true),
        DataTypes.createStructField("segunda_prueba", DataTypes.DoubleType, true),
        DataTypes.createStructField("estructura_anidada", DataTypes.createStructType(Array(
          DataTypes.createStructField("primera_prueba_anidada", DataTypes.StringType, true),
          DataTypes.createStructField("segunda_prueba_anidada", DataTypes.IntegerType, true)
        )), true),
        DataTypes.createStructField("array_de_prueba",DataTypes.createArrayType(DataTypes.StringType), true)

      ))
    )

    import es.us.idea.adt.spark.implicits._

    val ds = spark.read.json(s"/home/alvaro/datasets/hidrocantabrico_split.json")
      .adt(
        new NamedField(
          "consumoMinFormat",
          new IterableField(
            "consumo",
            new DataStructure(
              new NamedField("minPot",
                new TypedData(
                  new StructureModifier(
                    new DataSequence(
                      new BasicField("potencias.p1"),
                      new BasicField("potencias.p2"),
                      new BasicField("potencias.p3")
                    ), functions.min
                  ), DataTypes.IntegerType
                )
              )
            )
          )
        ),
        new NamedField(
          "tariff",
          new BasicField("tarifa")
        ),
        new NamedField("consumoFormateado", new IterableField("consumo", new DataStructure(new NamedField("pot1", new BasicField("potencias.p1")), new NamedField("pot2", new BasicField("potencias.p2")), new NamedField("pot3", new BasicField("potencias.p3")), new NamedField("all", new BasicField("potencias"))))),
        new NamedField("C_max", new IterableField("consumo", new DataSequence(
          new TypedData(new StructureModifier(new DataSequence(new BasicField("potencias.p1"), new BasicField("potencias.p4")), functions.max), DataTypes.IntegerType),
          new TypedData(new StructureModifier(new DataSequence(new BasicField("potencias.p2"), new BasicField("potencias.p5")), functions.max), DataTypes.IntegerType),
          new TypedData(new StructureModifier(new DataSequence(new BasicField("potencias.p3"), new BasicField("potencias.p6")), functions.max), DataTypes.IntegerType)
        )))

      )

    ds.explain(true)
    ds.show(false)
    ds.printSchema()

    //ds
    //  .t(shift from "consumo.*.p1" to "C.[&1].[0]", shift from "consumo.*.p1" to "C.[&1].[1]")

    //println(ds.schema.fields.filter(_.name=="consumo").head)
    //println(ds.schema.prettyJson)

    spark.close()
  }
}
