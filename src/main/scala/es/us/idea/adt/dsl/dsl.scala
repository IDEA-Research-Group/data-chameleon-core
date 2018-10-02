package es.us.idea.adt.dsl

import es.us.idea.adt.dsl.dsl.NamedDataSelector

object dsl {


  implicit class StrToSyntax(str: String){
    def :=(dataSelector: DataSelector*)= {
      NamedDataSelector(str, dataSelector: _*)
    }
  }

  implicit def strToDataSelector(str: String) = BasicSelector(str)


  trait DataSelector {
    def ->() : DataSelector
    def ->(dataSelector: DataSelector*) : DataSelector
    def *->() : DataSelector
    def *->(dataSelector: DataSelector*) : DataSelector
  }

  case class BasicSelector(path: String) extends DataSelector {
    override def ->(): DataSelector = ???
    override def ->(dataSelector: DataSelector*): DataSelector = ???

    override def *->(): DataSelector = ???
    override def *->(dataSelector: DataSelector*): DataSelector = ???
  }

  case class NestedDataSelector(dataSelector: DataSelector*) extends DataSelector {
    override def ->(): DataSelector = ???
    override def ->(dataSelector: DataSelector*): DataSelector = ???

    override def *->(): DataSelector = ???
    override def *->(dataSelector: DataSelector*): DataSelector = ???
  }

  case class NamedDataSelector(name: String, dataSelector: DataSelector*) extends DataSelector {
    override def ->(): DataSelector = ???
    override def ->(dataSelector: DataSelector*): DataSelector = ???

    override def *->(): DataSelector = ???
    override def *->(dataSelector: DataSelector*): DataSelector = ???
  }

  def max(dataSelector: DataSelector*): DataSelector = ???
  def avg(dataSelector: DataSelector*): DataSelector = ???

  def main(args: Array[String]) = {

    val test1 =
      "T" := "tarifa" // falta meter la transformación del diccionario

    val test2 =
      "CP" := (
        "p1" := max("potenciaContratada.p1", "potenciaContratada.p4"),
        "p1" := max("potenciaContratada.p2", "potenciaContratada.p5"),
        "p1" := max("potenciaContratada.p3", "potenciaContratada.p6")
      )

    val test3 =
      "PC" := "consumo" *-> (
        "p1" := max("potencias.p1", "potencias.p4"),
        "p2" := max("potencias.p2", "potencias.p5"),
        "p3" := max("potencias.p3", "potencias.p6")
      )

    val test4 =
      "AVG_PC" := "consumo" *-> (
        "p1" := max("potencias.p1", "potencias.p4"),
        "p2" := max("potencias.p2", "potencias.p5"),
        "p3" := max("potencias.p3", "potencias.p6")
      ) -> (
        "p1" := avg("p1"),
        "p2" := avg("p2"),
        "p3" := avg("p3")
      )





    println("aa")
  }
}
