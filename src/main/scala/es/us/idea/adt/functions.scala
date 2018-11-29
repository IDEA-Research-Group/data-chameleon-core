package es.us.idea.adt

import org.apache.spark.sql.types.{DataType, DataTypes}
import data.utils._
import es.us.idea.adt.data.functions.{ADTDataFunction, ADTReductionFunction}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try

object functions {

  object reductionFunctions {
    val max = ADTReductionFunction((seq: Seq[Any]) => {
      seq.maxOpt()
    }, DataTypes.DoubleType)

    val min = ADTReductionFunction((seq: Seq[Any]) => {
      seq.minOpt()
    }, DataTypes.DoubleType)

    val sum = ADTReductionFunction((seq: Seq[Any]) => {
      seq.sumOpt()
    }, DataTypes.DoubleType)

    val avg = ADTReductionFunction((seq: Seq[Any]) => {
      seq.avgOpt()
    }, DataTypes.DoubleType)
  }

  val asDouble = ADTDataFunction((value: Any) => TypeConversions.asDouble(value), DataTypes.DoubleType)
  val asInt = ADTDataFunction((value: Any) => TypeConversions.asInt(value), DataTypes.IntegerType)
  val asString = ADTDataFunction((value: Any) => TypeConversions.asString(value), DataTypes.StringType)
  val asLong = ADTDataFunction((value: Any) => TypeConversions.asLong(value), DataTypes.LongType)

  def times(literal: Int) = ADTDataFunction((value: Any) => {
    Transformations.times(value, Left(literal))
  }, None)

  def times(literal: Double) = ADTDataFunction((value: Any) => {
    Transformations.times(value, Right(literal))
  }, DataTypes.DoubleType)

  def asDate(format: String) = ADTDataFunction((value: Any) => {
    val process = (v: Any) => v match {
      case str: String => {
        val formatter: DateTimeFormatter = DateTimeFormat.forPattern(format)
        Try(new java.sql.Date(formatter.parseDateTime(str).getMillis)).toOption
      }
      case _ => None
    }

    value match {
      case Some(v) => process(v)
      case v => process(v)
    }

  }, DataTypes.DateType)

}
