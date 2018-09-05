package es.us.idea.adt.dsl

object DSL extends {


  trait ATContainer

  object shift {
    def from(from: String) = ShiftFromContainer(from)
  }

  case class Shift()

  case class ShiftFromContainer(from: String) {
    def to(to: String) = ShiftContainer(from, to)
  }
  case class ShiftContainer(from: String, to: String) extends ATContainer


  //implicit class StringFrom(from)


  def main(args: Array[String]) = {
    println("Aaa")

    shift from "aa" to "fasd"

  }
}
