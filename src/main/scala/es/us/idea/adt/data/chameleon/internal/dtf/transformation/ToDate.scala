package es.us.idea.adt.data.chameleon.internal.dtf.transformation

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.data.simple.{DateType, DoubleType}
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try

class ToDate(eval: Evaluable, format: String) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    val process = (v: Any) => v match {
      case str: String => {
        val formatter: DateTimeFormatter = DateTimeFormat.forPattern(format)
        Try(new java.sql.Date(formatter.parseDateTime(str).getMillis)).toOption
      }
      case _ => None
    }
    process(utils.unwrap(eval.getValue(in)))
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = new DateType()
    this.dataType = Some(dt)
    dt
  }
}
