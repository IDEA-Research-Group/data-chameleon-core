package es.us.idea.adt

import es.us.idea.adt.dsl.Composite2
import es.us.idea.adt.dsl.Composite2._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes


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

    val ds = spark.read.json(s"/home/alvaro/datasets/hidrocantabrico_split.json")
    //val ds = spark.read.json(s"/home/alvaro/datasets/prueba_arrays.json")
    //    .withColumn("out", explode(array(dataMappingUdf(struct($"consumo")))))

    // val ds2 = ProtoADT.protoADT(ds,
    //   new DataStructure(
    //     new NamedField("C",
    //       new IterableField("consumo",
    //         new DataSequence(
    //           new BasicField("potencias.p1"),
    //           new BasicField("potencias.p2"),
    //           new BasicField("potencias.p3"))
    //       )
    //     )
    //   )
    // )

    val ds2 = ProtoADT.protoADT(ds,
      //new DataStructure(
      //  new NamedField("consumoFormateado", new IterableField("consumo", new DataStructure(new NamedField("pot1", new BasicField("potencias.p1")), new NamedField("pot2", new BasicField("potencias.p2")), new NamedField("pot3", new BasicField("potencias.p3")), new NamedField("all", new BasicField("potencias")))))
      //)
      new DataStructure(
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
                    ), Composite2.min
                  ), DataTypes.IntegerType
                )
              )
            )
          )
        )
      )

    )

    ds2.show(false)
    ds2.printSchema()

    //ds
    //  .t(shift from "consumo.*.p1" to "C.[&1].[0]", shift from "consumo.*.p1" to "C.[&1].[1]")

    //println(ds.schema.fields.filter(_.name=="consumo").head)
    //println(ds.schema.prettyJson)

    spark.close()
  }
}
