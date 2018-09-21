package es.us.idea.adt.dsl

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.Try

object Composite2 {

  val schemaJson = "{\"type\":\"struct\",\"fields\":[{\"name\":\"DH\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ICPInstalado\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"consumo\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"anio\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"diasFacturacion\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaFinLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaInicioLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potencias\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"cups\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosAcceso\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"distribuidora\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaAltaSuministro\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaLimiteDerechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimaLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoCambioComercial\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoMovimientoContrato\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"impagos\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"importeGarantia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxActa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxBie\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potenciaContratada\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"precioTarifa\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadEqMedidaTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadICPTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tarifa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoFrontera\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoPerfil\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularTipoPersona\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularViviendaHabitual\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"totalFacturaActual\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionCodigoPostal\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionPoblacion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionProvincia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
  val schema = DataType.fromJson(schemaJson) match {
    case structType: StructType => structType
    case _ => throw new Exception("Couldn't treat the schema as a StructType")
  }
  //schema.
  //val schema = StructType.fromDDL(schemaDdl)

  val inTipo = Map(
    "id" -> 500,
    "array" -> Seq(0.0, 1.0, null, 80, null),
    "ejemplo" -> Seq(Seq(0.0, 1.0, 3.4), Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(null, 1.0), null, Seq(9.8, 1.0) ),
    "ejemplo2" -> Seq(Seq(Seq(1, "2"),Seq(1, "2"),Seq(1, "2")),Seq(Seq(1, "2"),Seq(1, "2"),Seq(1, "2")),Seq(Seq(1, "2"),Seq(1, "2"),Seq(1, "2"))),
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


  //trait ADTType // Para TypedData

  //val anyToOptDouble = (a: Any) => {
  //  a match {
  //    case Some(d: Double) => Some(d)
  //    case Some(i: Int) => Some(i.toDouble)
  //    case Some(f: Float) => Some(f.toDouble)
  //    case Some(l: Long) => Some(l.toDouble)
  //    case Some(s: String) => Try(s.toDouble).toOption // FIXME code smell
  //    case _ => None
  //  }
  //}

  implicit class SequencesAndOptions(seq: Seq[Any]) {

    def applyOperationOpt(f: (Double, Double) => Double, initialValue: Option[Double]=None): Option[Double] = {
      initialValue match {
        case None => seq.map(asDouble).reduceLeft((x, y) => x.flatMap(_x => y.map(_y => f(_x, _y))))
        case _ => seq.foldLeft(initialValue){ case (acc, el) => asDouble(el).flatMap(value => acc.map(ac => f(ac, value)))}
      }
    }

    def sumOpt()(implicit ev: Numeric[Double])= {
      applyOperationOpt(ev.plus, Some(ev.zero))
    }

    def maxOpt()(implicit ev: Numeric[Double]) = {
      applyOperationOpt(ev.max)
    }

    def minOpt()(implicit ev: Numeric[Double]) = {
      applyOperationOpt(ev.min)
    }

  }

  /**
    * Type conversions
    * */
  def asDouble(value: Any): Option[Double] = {
    val f =
      (a: Any) =>
        a match {
        case s: String => Try(s.toDouble).toOption
        case i: Int => Some(i.toDouble)
        case l: Long => Some(l.toDouble)
        case f: Float => Some(f.toDouble)
        case d: Double => Some(d)
        case _ => None
      }

    value match {
      case Some(a: Any) => f(a)
      case _ => f(value)
    }

  }

  def asInt(value: Any): Option[Int] = asDouble(value).map(a => a.toInt)

  def asString(value: Any): Option[String] = Some(value.toString)

  def asLong(value: Any): Option[Long] = asDouble(value).map(a => a.toLong)


  trait ADTSchema extends Serializable {
  }

  class ADTStructField(structField: StructField) extends ADTSchema {
    def get = structField


    override def equals(that: Any): Boolean = {
      println("aaa")

      that match {
        case that: ADTStructField => that.get.equals(this.get)
        case _ => false
      }
    }

    override def hashCode(): Int = {
      41 * (41 + structField.hashCode())
    }

  }

  class ADTDataType(dataType: DataType) extends ADTSchema {
    def get = dataType

    override def equals(that: Any): Boolean = {
      that match {
        case that: ADTDataType => that.get.equals(this.get)
        case _ => false
      }
    }

    override def hashCode(): Int = {
      41 * (41 + dataType.hashCode())
    }

  }


  trait Data extends Serializable {
    def getValue(in: Any): Any
    def getSchema(schm: ADTSchema): ADTSchema
  }

  class BasicField(path: String) extends Data {
    override def getValue(in: Any): Any = {
      in match {
        case m: Map[String, Any] =>  ADTDSL.recursiveGetValueFromPath(path, m) //ADTDSL.recursiveGetValueFromPath(path,in.asInstanceOf[Map[String, Any]])
        case _ => None
      }
    }

    override def getSchema(schm: ADTSchema): ADTSchema = {

      val resSchm = ADTDSL.recursiveGetSchemaFromPath(path.split('.'), schm)

      resSchm match {
        case adtStructField: ADTStructField => new ADTDataType(adtStructField.get.dataType)
        case _ => resSchm
      }
    }
  }

  class Default(value: Any, dataType: DataType) extends Data {
    override def getValue(in: Any): Any = value

    override def getSchema(schm: ADTSchema): ADTSchema = new ADTDataType(dataType)
  }

  class NamedField(name: String, data: Data) extends Data {
    override def getValue(in: Any): Any = data.getValue(in)

    override def getSchema(schm: ADTSchema): ADTSchema = {
      data.getSchema(schm) match {
        case adtStructField: ADTStructField => new ADTStructField(DataTypes.createStructField(name, DataTypes.createStructType(Array(adtStructField.get)), true))
        case adtDataType: ADTDataType => new ADTStructField(DataTypes.createStructField(name, adtDataType.get, true))
        case _ => throw new Exception()
      }
    }
  }

  class TypedData(data: Data, dataType: DataType) extends Data {
    override def getValue(in: Any): Any = {
      val value = data.getValue(in)
      dataType match {
        case _: DoubleType =>  println("double"); asDouble(value)
        case _: IntegerType => println("integer"); asInt(value)
        case _: StringType =>  println("string"); asString(value)
        case _: LongType =>    println("long"); asLong(value)
        case _ => None
      }
    }

    override def getSchema(schm: ADTSchema): ADTSchema = new ADTDataType(dataType)
  }

  class IterableField(path: String, dataUnion: DataUnion) extends Data {
    override def getValue(in: Any): Any = {

      val processMap = (m: Map[String, Any]) => {
        ADTDSL.recursiveGetValueFromPath(path, m)
          .map(s => s match {
              case seq: Seq[Any] => seq.map(v => dataUnion.getValue(v))
              case _ => None
            }
          )
      }

      in match {
        case m: Map[String, Any] => processMap(m)
        case _ => None
      }
    }

    //override def getSchema(schm: ADTSchema): ADTSchema = ???
    override def getSchema(schm: ADTSchema): ADTSchema = {

      val pathSchm = ADTDSL.recursiveGetSchemaFromPath(path.split('.'), schm)

      pathSchm match {
        case adtStructField: ADTStructField => adtStructField.get.dataType match {
          case arrayType: ArrayType => dataUnion.getSchema(new ADTDataType(arrayType.elementType)) match {
            case _adtDataType: ADTDataType => new ADTDataType(DataTypes.createArrayType(_adtDataType.get))
            case _ => throw new Exception("El campo anidado no devolvió un DataType")
          }
          case _ => throw new Exception("Has llamado a IterableField y no le has pasado un IterableField!!!!!!!!!!!")
        }
      }
    }
  }

  abstract class DataUnion(data: Data*) extends Data {
    override def getValue(in: Any): Any = data.map(_.getValue(in))
  }

  class DataSequence(data: Data*) extends DataUnion(data: _*) {
    //override def getValue(in: Any): Any = data.map(_.getValue(in))

    override def getSchema(schm: ADTSchema): ADTSchema = {
      // Todos los datos deben ser del mismo tipo

      val d = data.map(_.getSchema(schm)).distinct

      if(d.isEmpty) new ADTDataType(DataTypes.createArrayType(DataTypes.NullType))
      else if(d.size equals 1)
        d.head match {
          case adtDataType: ADTDataType => new ADTDataType(DataTypes.createArrayType(adtDataType.get))
          case adtStructField: ADTStructField => new ADTDataType(DataTypes.createArrayType(adtStructField.get.dataType))
          case _ => throw new Exception()
        }
      else new ADTDataType(DataTypes.StringType)

    }
  }

  class DataStructure(data: Data*) extends DataUnion(data: _*) {
    // TODO OJO 2 en el getValue hay que examinar todos los posibles campos anidados, a fin de transformar los Maps en Rows

    override def getValue(in: Any): Any = Row.apply(data.map(d => ADTDSL.findAndReplaceMaps(d.getValue(in))  ): _*)

    override def getSchema(schm: ADTSchema): ADTSchema = {
      new ADTDataType(
        DataTypes.createStructType(
          data.zipWithIndex.map(t => t._1.getSchema(schm) match {
            case adtStructField: ADTStructField => adtStructField.get
            case adtDataType: ADTDataType => DataTypes.createStructField(t._2.toString, adtDataType.get, true)
            case _ => throw new Exception("Tipò de dato nmo reconocido")
          }).toArray
        )
      )
    }
  }

  class StructureModifier(data: DataUnion, operation: (Seq[Any] => Any, DataType) ) extends Data {
    override def getValue(in: Any): Any = {
      data.getValue(in) match {
        case seq: Seq[Any] => operation._1(seq)
        case row: Row => operation._1(row.toSeq)
        case _ => None
      }
    }

    override def getSchema(schm: ADTSchema): ADTSchema = {
      new ADTDataType(operation._2)
    }
  }

  val max = ((seq: Seq[Any]) => {
    seq.maxOpt()
  }, DataTypes.DoubleType)

  val min = ((seq: Seq[Any]) => {
    seq.minOpt()
  }, DataTypes.DoubleType)

  val sum = ((seq: Seq[Any]) => {
    seq.sumOpt()
  }, DataTypes.DoubleType)


//  class ShapeOf(shape: Data, data: Data) extends Data {
//    override def getValue(in: Any): Any = {
//      val shapeVal = shape.getValue(in)
//
//
//
//      shapeVal match {
//
//        case s: Seq[Any] => s.map(x => getValue(x))
//        case m => Map[String, Any]
//
//      }
//
//    }
//
//    override def getSchema(schm: ADTSchema): ADTSchema = {
//      shape.getSchema(schm)
//    }
//  }


  def main(args: Array[String]): Unit = {
    val f = new BasicField("tarifa")
    val nf = new NamedField("tariff", new BasicField("tarifa"))
    val cnf = new NamedField("tariff", new NamedField("secondLevel", new BasicField("tarifa")))
    val deft = new NamedField("defaultValue", new NamedField("secondLevel", new Default(20, DataTypes.IntegerType)))
    val dsf = new NamedField("dataSeq", new DataSequence(new BasicField("potenciaContratada.p1"), new BasicField("potenciaContratada.p2")))
    val iterf = new IterableField("consumo", new DataSequence(new BasicField("potencias.p1"), new BasicField("potencias.p2"), new BasicField("potencias.p3")))
    val niterf = new NamedField("C", new IterableField("consumo", new DataSequence(new BasicField("potencias.p1"), new BasicField("potencias.p2"), new BasicField("potencias.p3"))))

    val datastrf = new DataStructure(new NamedField("pot1", new BasicField("potenciaContratada.p1")), new NamedField("pot2", new BasicField("potenciaContratada.p2")), new NamedField("pot3", new BasicField("potenciaContratada.p3")))
    val iterdatastrf = new NamedField("consumoFormateado", new IterableField("consumo", new DataStructure(new NamedField("pot1", new BasicField("potencias.p1")), new NamedField("pot2", new BasicField("potencias.p2")), new NamedField("pot3", new BasicField("potencias.p3")), new NamedField("all", new BasicField("potencias")))))

    val maxnf = new NamedField("max_pot_contratada", new StructureModifier(new DataSequence(new BasicField("potenciaContratada.p1"), new BasicField("potenciaContratada.p2"), new BasicField("potenciaContratada.p3")), max))

    val typedmin = new NamedField(
      "consumoMinFormat",
      new IterableField(
        "consumo",
        new DataStructure(
          new NamedField("minPot",
            new TypedData(
              new StructureModifier(
                new DataSequence(
                  new BasicField("potencias.p1"),
                  new BasicField("potencias.p2"),
                  new BasicField("potencias.p3")
                ), min
              ), DataTypes.IntegerType
            )
          )
        )
      )
    )

    val generatedSchema = typedmin.getSchema(new ADTStructField(DataTypes.createStructField("root", schema, true)))

    println(typedmin.getValue(inTipo))

    generatedSchema match {
      case adtStructField: ADTStructField => println(adtStructField.get)
      case adtDataType: ADTDataType => println(adtDataType.get)
      case _ => println("Nothing")
    }

  }

}
