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

    import spark.implicits._
    import es.us.idea.adt.spark.implicits._

    val ds = spark.read.json(s"/home/alvaro/datasets/hidrocantabrico_split.json")
      //.select("tarifa", "potenciaContratada", "consumo")
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
        new NamedField("PC_max",
          new DataSequence(
            new StructureModifier(
              new DataSequence(
                new BasicField("potenciaContratada.p1"),
                new BasicField("potenciaContratada.p4")
              ), functions.max
            ),
            new StructureModifier(
              new DataSequence(
                new BasicField("potenciaContratada.p2"),
                new BasicField("potenciaContratada.p5")
              ), functions.max
            ),
            new StructureModifier(
              new DataSequence(
                new BasicField("potenciaContratada.p3"),
                new BasicField("potenciaContratada.p6")
              ), functions.max
            )
          )
        ),
        new NamedField("consumoFormateado", new IterableField("consumo", new DataStructure(new NamedField("pot1", new BasicField("potencias.p1")), new NamedField("pot2", new BasicField("potencias.p2")), new NamedField("pot3", new BasicField("potencias.p3")), new NamedField("all", new BasicField("potencias"))))),
        new NamedField("C_max", new IterableField("consumo", new DataSequence(
          new TypedData(new StructureModifier(new DataSequence(new BasicField("potencias.p1"), new BasicField("potencias.p4")), functions.max), DataTypes.IntegerType),
          new TypedData(new StructureModifier(new DataSequence(new BasicField("potencias.p2"), new BasicField("potencias.p5")), functions.max), DataTypes.IntegerType),
          new TypedData(new StructureModifier(new DataSequence(new BasicField("potencias.p3"), new BasicField("potencias.p6")), functions.max), DataTypes.IntegerType)
        )))
      )
        //.select("PC_max")
    ds.show(false)
    ds.printSchema()

    spark.close()
  }
}
