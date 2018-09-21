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
}
