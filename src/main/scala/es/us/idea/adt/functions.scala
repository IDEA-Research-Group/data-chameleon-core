package es.us.idea.adt

import org.apache.spark.sql.types.DataTypes
import data.utils._

object functions {

  val max = ((seq: Seq[Any]) => {
    seq.maxOpt()
  }, DataTypes.DoubleType)

  val min = ((seq: Seq[Any]) => {
    seq.minOpt()
  }, DataTypes.DoubleType)

  val sum = ((seq: Seq[Any]) => {
    seq.sumOpt()
  }, DataTypes.DoubleType)

  val avg = ((seq: Seq[Any]) => {
    seq.avgOpt()
  }, DataTypes.DoubleType)

  val asDouble = ((value: Any) => TypeConversions.asDouble(value), Some(DataTypes.DoubleType))
  val asInt = ((value: Any) => TypeConversions.asInt(value), Some(DataTypes.IntegerType))
  val asString = ((value: Any) => TypeConversions.asString(value), Some(DataTypes.StringType))
  val asLong = ((value: Any) => TypeConversions.asLong(value), Some(DataTypes.LongType))

  def times(literal: Int) = ((value: Any) => {
    Transformations.times(value, Left(literal))
  }, None)

  def times(literal: Double) = ((value: Any) => {
    Transformations.times(value, Right(literal))
  }, Some(DataTypes.DoubleType))

}
