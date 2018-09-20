package es.us.idea.adt.dsl

import es.us.idea.adt.dsl.Composite2.{ADTDataType, ADTSchema, ADTStructField}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.Try

object ADTDSL {

  val schemaJson = "{\"type\":\"struct\",\"fields\":[{\"name\":\"DH\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ICPInstalado\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"consumo\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"anio\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"diasFacturacion\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaFinLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaInicioLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potencias\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"cups\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosAcceso\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"distribuidora\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaAltaSuministro\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaLimiteDerechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimaLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoCambioComercial\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoMovimientoContrato\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"impagos\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"importeGarantia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxActa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxBie\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potenciaContratada\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"precioTarifa\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadEqMedidaTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadICPTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tarifa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoFrontera\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoPerfil\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularTipoPersona\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularViviendaHabitual\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"totalFacturaActual\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionCodigoPostal\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionPoblacion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionProvincia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
  //val schema = DataType.fromJson(schemaJson)
  val schema = DataType.fromJson(schemaJson) match {
    case structType: StructType => structType
    case _ => throw new Exception("Couldn't treat the schema as a StructType")
  }

  val inTipo = Map(
    "id" -> 500,
    "array" -> Seq(0.0, 1.0, null, 80, null),
    "ejemplo" -> Seq(Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(null, 1.0), null, Seq(9.8, 1.0) ),
    //"ejemplo" -> Seq(),
    //"ejemplo" -> Seq(Seq()),
    "ICPInstalado" -> "Icp no instalado", "derechosExtension" -> 32.91, "tension" -> "3X220/380V", "propiedadEqMedidaTitular" -> "Empresa distribuidora",
    "potenciaContratada" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 34.91, "p2" -> 32.91, "p1" -> 32.91, "p6" -> 0.0), "impagos" -> "NO", "tipoFrontera" -> 4,
    "tarifa" -> "3.0A", "ubicacionPoblacion" -> "SOMO", "potMaxBie" -> 32.91, "distribuidora" -> "0027 - VIESGO DISTRIBUCION ELECTRICA, S.L.", "fechaAltaSuministro" -> "24/04/1991", "DH" -> "DH3", "totalFacturaActual" -> 4098.68,
    "propiedadICPTitular" -> "Empresa distribuidora", "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria",
    "consumo" -> Seq(
      Map("arrayAnidado" -> Seq(1.0, 5.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014"),
      Map("arrayAnidado" -> Seq(2.0, 10.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 10.0, "p2" -> 20.0, "p1" -> 20.0, "p6" -> 0.0), "anio" -> 2015, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")
    ),
    "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> 32.91)




  // def unionPath(path: String, )


  //def pathInterpreter(path: String)


  def getValueFromMap(path: Seq[String], map: Map[String, Any]): Option[Any] = {
    println(path)
    val current = path.head
    val nextPath = path.tail

    val value = map.get(current)
    if(nextPath.isEmpty) return value
    else {
      if(nextPath.head.equals("*")) {
        val seqOpt = Try(map(current).asInstanceOf[Seq[Any]]).toOption
        if(seqOpt.isDefined) return getValuesFromSeq(nextPath.tail, seqOpt.get)
        else return seqOpt
      } else {
        val mapOpt = Try(map(current).asInstanceOf[Map[String, Any]]).toOption
        if(mapOpt.isDefined) return getValueFromMap(nextPath.tail, mapOpt.get)
        else return mapOpt
      }
    }
  }

  def getValuesFromSeq(path: Seq[String], seq: Seq[Any]): Option[Any] = {

    val current = path.head
    val nextPath = path.tail


    seq.map(m => {
      if(current.equals("*")) {
        m.asInstanceOf[Seq[Any]]
      }



    })


    print(seq)
    None
  }

  def recursiveGetValueFromPath(path: String, map: Map[String, Any]): Option[Any] = {
    if (path.contains(".")) {
      val keySplt = path.split('.')
      if (map.contains(keySplt.head)) {
        return recursiveGetValueFromPath(keySplt.tail.mkString("."), map(keySplt.head).asInstanceOf[Map[String, Any]])
      } else {
        return None
      }
    } else {
      return map.get(path) // .getOrElse(null)
    }
  }
/*
  @Deprecated
  def recursiveGetSchemaFromPathOld(path: Seq[String], structField: StructField): StructField = {

    //product match {
    //  case structType: StructType => println("Structtype"); path.headOption.flatMap(k => structType.fields.find(_.name == k).map(_structField => if(path.length >1) recursiveGetSchemaFromPath(path.tail, _structField) else _structField)).getOrElse(product)
    //  //case arrayType: ArrayType => println("Arraytype")
    //  case structField: StructField => println("StructField"); //recursiveGetSchemaFromPath(path, structField.)
    //    structField.dataType match {
    //      case structType2: StructType => recursiveGetSchemaFromPath(path, structType2)
    //      case _ => product
    //    }
    //  case _ => product
    //}

    path.headOption.flatMap(k => structField.dataType match {
      case structType: StructType => structType.fields.find(_.name == k)
        .map(_structField => if(path.length > 1){recursiveGetSchemaFromPath(path.tail, _structField)} else _structField)
      case _ => Some(structField)
    }).getOrElse(structField)




//    if(path.contains(".")) {
//      val keySplt = path.split('.')
//      // comprobar si el field es struct una vez que lo saco. Si es asi, llamada recursiva. Si no, devolverlo. Repetir esto hasta que el path se acabe.
//      val fda = keySplt.headOption.flatMap(k =>  structType.fields.filter(_.name == k).headOption.map(s =>
//        if(keySplt.length > 1){
//          s.dataType match {
//            case _structType: StructType => recursiveGetSchemaFromPath(keySplt.tail.mkString("."), _structType)
//            case _ => s
//          }
//        } else s
//      )).getOrElse()
//    }

    //schema match {
    //  case structType: StructType => structType.fields.filter() //structType.fields.filter(_.name == path).map(_).headOption.getOrElse(structType)
    //  //case _ => println("No match"); Some(schema)
    //  //case structField: StructField =>
    //  //case arrayType: ArrayType =>
    //}
  }
*/
  // esta función está preparada para procesar StructField o StructType.
  def recursiveGetSchemaFromPath(path: Seq[String], schema: ADTSchema): ADTSchema = {

    val processStructType = (key: String, st: StructType) => {
      st.fields.find(_.name == key)
        .map(_structField => if(path.length > 1){recursiveGetSchemaFromPath(path.tail, new ADTStructField(_structField))} else new ADTStructField(_structField))
    }

    val processDataType = (key: String, dataType: DataType) => {
      dataType match {
        case structType: StructType => processStructType(key, structType)
        case _ => throw new Exception("Error en el tipo de dato de entrada")
      }
    }

    path.headOption.flatMap(k => {
      schema match {
        case adtStructField: ADTStructField => processDataType(k, adtStructField.get.dataType)
        case adtDataType: ADTDataType => processDataType(k, adtDataType.get)
        case _ => throw new Exception("Tipo de dato no reconocible")
      }
    }).getOrElse(schema)
  }

  def findAndReplaceMaps(in: Any): Any = {

    val process = (a: Any) => {
      a match {
        case map: Map[String, Any] => Row.apply(map.toArray.map(t => findAndReplaceMaps(t._2)): _*)
        case seq: Seq[Any] => seq.map(e => findAndReplaceMaps(e))
        case any => any
      }
    }

    in match {
      case Some(x: Any) => process(x)
      case _ => process(in)
    }
  }



  def main(args: Array[String]) = {

    //println(getValueFromMap("consumo.*.potencias.p1".split('.'), inTipo))

    //println(schema.fields.filter(_.name == "potenciaContratada").headOption.flatMap(x =>
    //  x.dataType match {
    //    case structType: StructType => structType.fields.filter(_.name == "p1").headOption
    //    case _ => println("No match");Some(x)
    //}))

    //val s = new ADTStructField(DataTypes.createStructField("root", schema, true))
    val s = new ADTDataType(schema)

    //println(recursiveGetSchemaFromPath("consumo".split('.'), DataTypes.createStructField("root", schema, true)))
    //println(recursiveGetSchemaFromPath("potenciaContratada".split('.'), DataTypes.createStructField("root", schema, true)))
    //println(recursiveGetSchemaFromPath("potenciaContratada.p1".split('.'), DataTypes.createStructField("root", schema, true)))
    //println(recursiveGetSchemaFromPath("root".split('.'), DataTypes.createStructField("root", schema, true)))
    println("Otro")
    println(recursiveGetSchemaFromPath("consumo".split('.'), s).asInstanceOf[ADTStructField].get)
    println(recursiveGetSchemaFromPath("potenciaContratada".split('.'), s).asInstanceOf[ADTStructField].get)
    println(recursiveGetSchemaFromPath("potenciaContratada.p1".split('.'), s).asInstanceOf[ADTStructField].get)
    //println(recursiveGetSchemaFromPath2("root".split('.'), s).asInstanceOf[ADTStructField].get)
    //println(recursiveGetSchemaFromPath("consumo".split('.'), schema))


  }




}
