package es.us.idea.adt.implicits

import es.us.idea.adt.dsl.DSL.ATContainer
import org.apache.spark.sql.DataFrame

object Helpers {


  implicit class DataFrameATImplicit(df: DataFrame) {
    def t(args: ATContainer*) = {

    }
  }


}
