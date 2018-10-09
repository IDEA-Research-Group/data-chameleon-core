package es.us.idea.adt.dsl

import es.us.idea.adt.functions
import org.apache.spark.sql.types.DataType

trait Modifiable extends Container{
  def times(n: Int) = {
    new DataModifierContainer(this)(functions.times(n))
  }
  def times(n: Double) = {
    new DataModifierContainer(this)(functions.times(n))
  }

  def /(f: (Any => Option[Any], Option[DataType])) = {
    new DataModifierContainer(this)(f)
  }

}
