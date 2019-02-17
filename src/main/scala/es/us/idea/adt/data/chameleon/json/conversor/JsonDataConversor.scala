package es.us.idea.adt.data.chameleon.json.conversor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType

object JsonDataConversor {

  import scala.collection.JavaConverters._

  /*
  * ARRAY,BINARY,BOOLEAN,MISSING,NULL,NUMBER,OBJECT,POJO,STRING
}*/

  def json2chameleon(jsonNode: JsonNode): Any = {

    jsonNode.getNodeType match {
      case JsonNodeType.ARRAY => jsonNode.elements().asScala.toList.map(x => json2chameleon(x))
      case JsonNodeType.OBJECT => jsonNode.fieldNames().asScala.map(name => (name, json2chameleon(jsonNode.get(name)))).toMap
      case _ => {
        if(jsonNode.isBigDecimal || jsonNode.isFloat || jsonNode.isDouble) Some(jsonNode.asDouble())
        else if(jsonNode.isBigInteger || jsonNode.isLong) Some(jsonNode.asLong())
        else if(jsonNode.isInt || jsonNode.isShort) Some(jsonNode.asInt())
        else if(jsonNode.isTextual) Some(jsonNode.asText())
        else None
      }
    }

  }

}
