package es.us.idea.adt.data

/** This abstract class represents a Union of several Data objects
  *
  * @param data the set of Data objects to be joint
  */
abstract class DataUnion(data: Data*) extends Data {

  /**
    *
    * @param in the source data structure where the value must be found
    * @return the extracted Data value
    */
  override def getValue(in: Any): Any = data.map(_.getValue(in))

  /**
    *
    * @return the first level path that points at this Data
    */
  override def getFirstLevelPath(): Seq[String] = data.flatMap(_.getFirstLevelPath())

}