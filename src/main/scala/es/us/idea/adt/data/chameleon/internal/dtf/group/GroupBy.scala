package es.us.idea.adt.data.chameleon.internal.dtf.group

import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}
import es.us.idea.adt.data.chameleon.data.complex.{ArrayType, StructType}
import es.us.idea.adt.data.chameleon.internal.{Evaluable, utils}
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator


// TODO debería aceptar un Evaluable como key
class GroupBy(evaluable: Evaluable, key: Evaluable, operation: Option[Evaluable] = None) extends DTFOperator {
  override var dataType: Option[DataType] = None

  override def getValue(in: Any): Any = {
    utils.unwrap(evaluable.getValue(in)) match {
      case seq: Seq[Any] => {
        val cleanSeq =
          seq.map(element => utils.unwrap(element) match {
            //case map: Map[String, Any] => Some((map.get(key), map))
            case map: Map[String, Any] => Some(map)
            case _ => None
          })
            .filter(_.isDefined)
            //.filter(_.get.get(key).isDefined)
            .map(_.get)
            .groupBy( x => utils.unwrap(key.getValue(x)))
            .toSeq

        operation match {
          case Some(op) => cleanSeq.map(x => Map("__key" -> x._1, "result" -> op.getValue(x._2) ))
          case _ => cleanSeq.map(x => Map("__key" -> x._1, "result" -> x._2 ))
        }
      }
      case _ => None
    }
  }

  override def evaluate(parentDataType: DataType): DataType = {

    val dt =
      evaluable.evaluate(parentDataType) match {
        case arrayType: ArrayType => {
          arrayType.getElementDataType match {
            case structType: StructType => {
              //val attribute = structType.findAttribute(key)

              new ArrayType(
                new StructType(
                  //new Attribute("__key", attribute.getDataType),
                  new Attribute("__key", key.evaluate(structType)),
                  new Attribute("result",
                    operation match {
                      case Some(op) => op.evaluate(evaluable.getDataType)
                      case _ => structType
                    }
                  )
                )
              )

              //operation match {
              //  case Some(ev) =>
              //    new ArrayType(
              //      new StructType(
              //        attribute, // TODO en caso de aceptar un evaluable como key, este atributo se llamaría _key
              //        new Attribute("result", ev.evaluate(evaluable.getDataType))
              //      )
              //    )
              //  case _ => {
              //    new ArrayType(
              //      new StructType(
              //        attribute,
              //        new Attribute("result", new ArrayType(
              //          structType
              //          //new StructType( structType.getAttributes.filter(x => x.getName != attribute.getName): _* )
              //        ))
              //      )
              //    )
              //  }
              //}
            }
            case _ => throw new Exception(s"GroupBy DTF must be performed over an ArrayType whose elements are StructType. Instead, it was $arrayType")
          }
        }
        case other => throw new Exception(s"GroupBy DTF must be performed over an ArrayType. Instead it was $other")
      }

    this.dataType = Some(dt)
    dt
  }
}
