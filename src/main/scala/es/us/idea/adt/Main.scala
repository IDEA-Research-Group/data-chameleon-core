package es.us.idea.adt

import es.us.idea.adt.data._
import es.us.idea.adt.data.functions.ADTReductionFunction
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.joda.time.{Days, LocalDate}

object Main {
  def main(args: Array[String]) = {

    //println("Hello World!")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark ADT")
      .getOrCreate()

    import es.us.idea.adt.spark.implicits._
    import es.us.idea.adt.dsl.implicits._
    import functions._

    val daysBetweenDates = ADTReductionFunction((s: Seq[Some[java.sql.Date]]) => {
      (s.headOption, s.lastOption) match {
        case (Some(endOpt), Some(startOpt)) => (endOpt, startOpt) match {
          case (Some(end), Some(start)) => Days.daysBetween(new LocalDate(start), new LocalDate(end)).getDays
          case _ => None
        }
        case _ => None
      }
    } , DataTypes.IntegerType)

    val ds = spark.read.json(s"/home/alvaro/datasets/hidrocantabrico_split.json")
      .adt(
        d"consumoMinFormat" * ("consumo" &+ d"minPot" < min("potencias.p1", "potencias.p2", "potencias.p3")),
        d"tariff" < "tarifa",
        d"PC_MAX" * (max("potenciaContratada.p1", "potenciaContratada.p4"), max("potenciaContratada.p2", "potenciaContratada.p5"), max("potenciaContratada.p3", "potenciaContratada.p4")),
        d"consumoFormateado" * ("consumo" &+ (
          (1 to 6).map(i => d(s"pot$i") < s"potencias.p$i"): _*
        )),
        d"C_MAX" * ("consumo" &* (
          (1 to 3).map(i => max(s"potencias.p$i", s"potencias.p${i+3}")) : _*
        )),
        d"avgConsumoPot" + (
          (1 to 3).map(i => d(s"p$i") < (avg("consumo" & max(s"potencias.p$i", s"potencias.p${i+3}")) / times(10000) / asInt)) : _*
        ),
        d"billingDays" * ("consumo" & "diasFacturacion"),
        d"consumoDate" * ("consumo" & ("fechaFinLectura" / asDate("dd/MM/yyyy"))),
        d"calculatedBillingDays" * ("consumo" & reduce("fechaFinLectura" / asDate("dd/MM/yyyy"), "fechaInicioLectura" / asDate("dd/MM/yyyy"))(daysBetweenDates))
      )

    ds.show(false)
    ds.printSchema()

    spark.close()
  }
}
