package es.us.idea.adt.data.exceptions


final case class UnsupportedDataStructureException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
