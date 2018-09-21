package es.us.idea.adt.data

abstract class DataUnion(data: Data*) extends Data {
  override def getValue(in: Any): Any = data.map(_.getValue(in))
}