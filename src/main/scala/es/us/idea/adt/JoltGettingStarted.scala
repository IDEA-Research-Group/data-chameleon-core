package es.us.idea.adt

import com.bazaarvoice.jolt.{Chainr, JsonUtils}

import scala.collection.JavaConverters.asJavaIterableConverter

object JoltGettingStarted {
  def main(args: Array[String]): Unit = {
    val any = List(
      Map("operation" -> "shift",
        "spec" -> Map(
          "tarifa" -> "T"
        )
      )
    ).asJava

    val a = JsonUtils.jsonToList( "[\n  {\n    \"operation\": \"shift\",\n    \"spec\": {\n      \"tarifa\": \"T\"}}]")

    val chain = Chainr.fromSpec(any)

    //chain.transform()

    println("terminado")
  }
}
