package es.us.idea.adt

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) = {

    println("Hello World!")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark ADT")
      .getOrCreate()

    import es.us.idea.adt.implicits.Helpers._
    import es.us.idea.adt.dsl.DSL._

    val ds = spark.read.json(s"/home/alvaro/datasets/hidrocantabrico_split.json")
    ds.printSchema()
    ds
      .t(shift from "consumo.*.p1" to "C.[&1].[0]", shift from "consumo.*.p1" to "C.[&1].[1]")

  }
}
