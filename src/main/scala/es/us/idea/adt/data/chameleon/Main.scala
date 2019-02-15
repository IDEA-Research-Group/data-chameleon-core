package es.us.idea.adt.data.chameleon

import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]) = {
    //args.headOption match {
    //  case Some(s) => execute(s)
    //  case _ => println("Error: No dataset path specified.")
    //}
  }

  execute("datasets/power_consumption.json")

  def execute(datasetPath: String) = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark ADT")
      .getOrCreate()

    import es.us.idea.adt.data.chameleon.dsl.implicits._
    import es.us.idea.adt.data.chameleon.spark.implicits._
    import es.us.idea.adt.data.chameleon.internal.dtfs._

    val translations = Seq("TA", "TB", "TC", "TD", "TE", "TF", "TG", "TH", "TI", "TJ", "TK", "TL", "TM").zipWithIndex.toMap

    // ADT User Defined Function to translate the "tariff" field
    //val translate = ADTDataFunction((s: String) => {
    //  translations.getOrElse(s, -1)
    //} , DataTypes.IntegerType)

    // ADT User Defined Function to calculate the number of days between the "startDate" and "endDate"
    //val daysBetweenDates = ADTReductionFunction((s: Seq[Some[java.sql.Date]]) => {
    //  (s.headOption, s.lastOption) match {
    //    case (Some(endOpt), Some(startOpt)) => (endOpt, startOpt) match {
    //      case (Some(end), Some(start)) => Days.daysBetween(new LocalDate(start), new LocalDate(end)).getDays
    //      case _ => None
    //    }
    //    case _ => None
    //  }
    //} , DataTypes.IntegerType)

    // Read the Dataset and apply the Data Transformation Functions
    val ds = spark.read.json(datasetPath)
        .chameleon(
          "ID" << t"customerID",
          "CP" << (array(
            array(t"contractedPower.period1", t"contractedPower.period4") -> max,
            array(t"contractedPower.period2", t"contractedPower.period5") -> max,
            array(t"contractedPower.period3", t"contractedPower.period6") -> max
          ) iterate t"." -> toInt),
          "Group" << (t"consumption" -> groupBy(t"power.period1", t"." -> count) iterate struct(
            "potencia1" << t"__key",
            "count" << t"result"
          ))
        )

    //val ds = spark.read.json(datasetPath)
    //  .adt(
    //    d"ID" < "customerID", //T1
    //    d"T" < "tariff" / translate, //T2
    //    d"CP" * ( //T3
    //      max("contractedPower.period1", "contractedPower.period4") / asInt,
    //      max("contractedPower.period2", "contractedPower.period5") / asInt,
    //      max("contractedPower.period3", "contractedPower.period6") / asInt
    //    ),
    //    d"C" * ("consumption" &* ( //T4
    //      (1 to 3).map(i => max(s"power.period$i", s"power.period${i+3}")) : _*
    //      )),
    //    d"AVG_C" + ( //T5
    //      (1 to 3).map(i => d(s"p$i") < avg("consumption" & max(s"power.period$i", s"power.period${i+3}"))) : _*
    //      ),
    //    d"BD" * ("consumption" & reduce("endDate" / asDate("dd/MM/yyyy"), "startDate" / asDate("dd/MM/yyyy"))(daysBetweenDates)) //T6
    //  )
    //  .select("ID", "T", "CP", "C", "AVG_C", "BD")

    // Show a preview of the Dataset and print its schema
    ds.show()
    ds.printSchema()

    spark.close()
  }

}
