package es.us.idea.adt.data

import es.us.idea.adt.data.schema.ADTSchema

/** This trait represent a specific Data in the data structure.
  *
  */
trait Data extends Serializable {

  /** Given an input data structure, extracts the queried value. The expected structure of the input data and the
    * strategy for looking for the target value depends on the class that implements this trait.
    *
    * @param in the source data structure where the value must be found
    * @return the extracted Data value
    */
  def getValue(in: Any): Any

  /** Given an input schema, extracts the expected result schema according to strategy used to look for the target
    * value.
    *
    * @param schema the source schema
    * @return the resulting schema
    */
  def getSchema(schema: ADTSchema): ADTSchema
}