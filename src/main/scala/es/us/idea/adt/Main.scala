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

    import  es.us.idea.adt.dsl.implicits._


    val ds = spark.read.json(s"/home/alvaro/datasets/hidrocantabrico_split.json")
      .adt(
        d"consumoMinFormat" < "consumo" &+ d"minPot" < min("potencias.p1", "potencias.p2", "potencias.p3"),
        d"tariff" < "tarifa",
        d"PC_MAX" * (max("potenciaContratada.p1", "potenciaContratada.p4"), max("potenciaContratada.p2", "potenciaContratada.p5"), max("potenciaContratada.p3", "potenciaContratada.p4")),
        d"consumoFormateado" < "consumo" &+ (
          d"pot1" < "potencias.p1",
          d"pot2" < "potencias.p2",
          d"pot3" < "potencias.p3",
          d"pot4" < "potencias.p4",
          d"pot5" < "potencias.p5",
          d"pot6" < "potencias.p6"
        ),
        d"C_MAX" < "consumo" &* (
          max("potencias.p1", "potencias.p4"),
          max("potencias.p2", "potencias.p5"),
          max("potencias.p3", "potencias.p6")
        ),
        d"avgConsumoPot" + (
          d"p1" < avg("consumo" & max("potencias.p1", "potencias.p4")),
          d"p2" < avg("consumo" & max("potencias.p2", "potencias.p5")),
          d"p3" < avg("consumo" & max("potencias.p3", "potencias.p6"))
        ),
        d"billingDays" < "consumo" & "diasFacturacion"
      )

    ds.show(false)
    ds.printSchema()

    spark.close()
  }
}
