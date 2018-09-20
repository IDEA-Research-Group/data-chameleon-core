package es.us.idea.adt.dsl

import es.us.idea.adt.Test.schemaJson
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types._

import scala.util.Try

object Composite {

  // IMPLICITS helpers
  implicit class SequencesAndOptions(seq: Seq[Any]) {

    def applyOperationOpt(f: (Double, Double) => Double, initialValue: Option[Double]=None): Option[Double] = {
      initialValue match {
        case None => seq.map(anyToOptDouble).reduceLeft((x, y) => x.flatMap(_x => y.map(_y => f(_x, _y))))
        case _ => seq.foldLeft(initialValue){ case (acc, el) => anyToOptDouble(el).flatMap(value => acc.map(ac => f(ac, value)))}
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

  val anyToOptDouble = (a: Any) => {
    a match {
      case Some(d: Double) => Some(d)
      case Some(i: Int) => Some(i.toDouble)
      case Some(f: Float) => Some(f.toDouble)
      case Some(l: Long) => Some(l.toDouble)
      case Some(s: String) => Try(s.toDouble).toOption // FIXME code smell
      case _ => None
    }
  }

  implicit class NotNullOption[T](val t: Try[T]) extends AnyVal {
    def toNotNullOption = t.toOption.flatMap{Option(_)}
  }

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

  abstract class Field() {
    //def getValue(in: Map[String, Any]): Any
    def getValue(in: Any): Any // El comportamiento debe cambiar segun el tipo de datos
    def getSchema(schema: StructField): StructField
    //def transform()
  }



  // TODO cambiar nombre
  //abstract class ArrayField() {
  //  def getSchema(schema: StructField): DataType
  //}


  class DefaultField(value: Any) extends Field {
    override def getValue(in: Any): Any = value

    override def getSchema(schema: StructField): StructField= ???
  }

  class BasicField(path: String) extends Field {
    override def getValue(in: Any): Any = {
      //println(in.getClass)
      in match {
        case m: Map[String, Any] =>  ADTDSL.recursiveGetValueFromPath(path, m) //ADTDSL.recursiveGetValueFromPath(path,in.asInstanceOf[Map[String, Any]])
        case _ => None
      }
    }

    //override def getSchema(schema: StructField): StructField = {
    //  ADTDSL.recursiveGetSchemaFromPath(path.split('.'), schema)
    //}

    override def getSchema(schema: StructField): StructField = ???

  }

  // fixme
  class IndexField(index: Int)  extends Field {
    override def getValue(in: Any): Any = {
      in match {
        case s: Seq[Any] => s.lift(index) //Try(in.asInstanceOf[Seq[Any]](index)).toNotNullOption
        case _ => None
      }
    }
    override def getSchema(schema: StructField): StructField= ???
  }


  class IterableField(field: Option[Field]) extends Field {

    // If it receives just a Field object, then it assumes that "in" is a Sequence.
    // If it receives a String specifying the path, it assumes that "in" is a Map

    //def this(path: String, field: Field) = this(Some(path), Some(field))
    def this(field: Field) = this(Some(field))
    //def this(path: String) = this(Some(path), None)
    def this() = this(None)

    override def getValue(in: Any): Any = {

      in match {
        //case m: Map[String, Any] => None
        case s: Seq[Any] => field.map(f => s.map(a => f.getValue(a))).getOrElse(s)
        case _ => None
      }

      //var s:Option[Seq[Any]] = None
      //if(path.isDefined)
      //  s = Try(ADTDSL.recursiveGetValueFromPath(path.get,in.asInstanceOf[Map[String, Any]]).get.asInstanceOf[Seq[Any]]).toNotNullOption
      //else
      //  s = Try(in.asInstanceOf[Seq[Any]]).toNotNullOption
      //if(s.isEmpty) None
      //else if(field.isDefined) s.get.map( m => field.get.getValue(m))
      //else s
    }

    //override def getSchema(schema: StructField): StructField = {
    //  DataTypes.createArrayType()
    //}
    //override def getSchema(schema: StructField): StructField = {
    //  val a = field.map(f => f.getSchema(schema).dataType)
    //}
    override def getSchema(schema: StructField): StructField = ???
  }

  class IterableFieldFromPath(path: Option[String], field: Option[Field]) extends IterableField(field) {

    def this(path: String, field: Field) = this(Some(path), Some(field))
    def this(field: Field) = this(None, Some(field))
    def this(path: String) = this(Some(path), None)
    def this() = this(None, None)


    override def getValue(in: Any): Any = {

      in match {
        //case m: Map[String, Any] => path.map(p => m(p)).getOrElse(m)  //m.get
        case m: Map[String, Any] => path.flatMap(p => {
          ADTDSL.recursiveGetValueFromPath(p, m).flatMap(s => {
            s match {
              case seq: Seq[Any] => field.map(f => seq.map(x => f.getValue(x)))
              case _ => None
            }
          })
        })
        case _ => None
      }




      //var s:Option[Seq[Any]] = None
      //if(path.isDefined)
      //  s = Try(ADTDSL.recursiveGetValueFromPath(path.get,in.asInstanceOf[Map[String, Any]]).get.asInstanceOf[Seq[Any]]).toNotNullOption
      //else
      //  s = Try(in.asInstanceOf[Seq[Any]]).toNotNullOption
      //if(s.isEmpty) None
      //else if(field.isDefined) s.get.map( m => field.get.getValue(m))
      //else s
    }

  }

//  class IterableFieldFromIndex(index: Option[Int], field: Option[Field]) extends IterableField {
//    def this(index: Int, field: Field) = this(Some(index), Some(field))
//    def this(field: Field) = this(None, Some(field))
//    def this(index: Int) = this(Some(index), None)
//    def this() = this(None, None)
//
//    override def getValue(in: Any): Any = {
//
//      in match {
//        case s: Seq[Any] => index.map(p => s(p)).getOrElse(s)  //m.get
//        //case s: Seq[Any] => None
//        case _ => None
//      }
//    }
//  }



  class SeqField(fields: Seq[Field] = Seq()) extends Field  {
    override def getValue(in: Any): Any = {
      fields.map(_.getValue(in))
    }

    //override def getSchema(schema: StructField): DataType = {
    //  val distinctDT = fields.map(f => f.getSchema(schema).dataType).distinct
    //  if(distinctDT.nonEmpty){
    //    if(distinctDT.length > 1) DataTypes.createArrayType(DataTypes.StringType)
    //    else DataTypes.createArrayType(distinctDT.head)
    //  } else DataTypes.NullType
    //}
    override def getSchema(schema: StructField): StructField = ???
    //def max(in: M)
  }

  // Modificadores estructurales
  class MaxField(fields: Seq[Field] = Seq()) extends Field {
    override def getValue(in: Any): Any = {
      fields.map(x => x.getValue(in)).maxOpt
    }
    override def getSchema(schema: StructField): StructField= ???
  }

  // Modificadores estructurales
  class MinField(fields: Seq[Field] = Seq()) extends Field {
    override def getValue(in: Any): Any = {
      fields.map(x => x.getValue(in)).minOpt
    }
    override def getSchema(schema: StructField): StructField= ???
  }

  /**
    * Given the field shape, fills all single values with the value specified in field field.
    * TODO Temporary unavailable
    */
  class ShapeOf(shape: Field, field: Field) extends Field {

    override def getValue(in: Any): Any = {
      // Get the shape field
      val shapeVal = shape.getValue(in)
      // Get the value to use
      val value = field.getValue(in)
      // Recursive function to fill shapeVal with value
      recursiveGetValue(shapeVal, value)
    }

    def recursiveGetValue(in: Any, value: Any): Any = {

      // Check if the entry value is a seq

      val s = in match {
        case _: Seq[Any] => Option(in.asInstanceOf[Seq[Any]])
        case _: Option[Seq[Any]] => in.asInstanceOf[Option[Seq[Any]]]
        case _ => None
      }

      if(s.isDefined) s.get.map(x => {
        if(x.isInstanceOf[Seq[Any]]) recursiveGetValue(x, value)
        else value
      })
      else None
    }
    override def getSchema(schema: StructField): StructField= ???
  }


  def main(args: Array[String]) = {
    val f = new BasicField("tarifa")

    //val ff = new BasicField("consumo") foreach new SeqField(Seq(new BasicField("potencias.p1"), new BasicField("potencias.p2"), new BasicField("potencias.p3")))

    val iff = new IterableFieldFromPath("consumo", new SeqField(Seq(new BasicField("potencias.p1"), new BasicField("potencias.p2"), new BasicField("potencias.p3"))))
    val maxf = new IterableFieldFromPath("consumo", new MaxField(Seq(new BasicField("potencias.p1"), new BasicField("potencias.p2"), new BasicField("potencias.p3"))))
    val minf = new IterableFieldFromPath("consumo", new MinField(Seq(new BasicField("potencias.p1"), new BasicField("potencias.p2"), new BasicField("potencias.p3"))))

    val matf = new IterableFieldFromPath("ejemplo", new SeqField(Seq(new IndexField(0), new IndexField(1))))
    val matf3 = new IterableFieldFromPath("ejemplo")
    val matf2 = new IterableFieldFromPath("ejemplo2", new IterableField(new SeqField(Seq(new IndexField(0), new IndexField(1)))))

    val pot = new SeqField(Seq(new BasicField("potenciaContratada.p1"), new BasicField("potenciaContratada.p2"), new BasicField("potenciaContratada.p3")))
    val potMax = new MaxField(Seq(new BasicField("potenciaContratada.p1"), new BasicField("potenciaContratada.p2"), new BasicField("potenciaContratada.p3")))

    val shapeOf = new ShapeOf(new IterableFieldFromPath("ejemplo"), new BasicField("tarifa"))
    val shapeOfCustom = new ShapeOf(iff, new BasicField("tarifa"))


    println(iff.getValue(inTipo))
    println(maxf.getValue(inTipo))
    println(minf.getValue(inTipo))
    println(matf.getValue(inTipo))
    //println(matf2.getValue(inTipo))
    //println(matf3.getValue(inTipo))
    //println(pot.getValue(inTipo))
    //println(potMax.getValue(inTipo))
    //println(shapeOf.getValue(inTipo))
    println(shapeOfCustom.getValue(inTipo))

    println(schema)

    //val x: Any = Map("a" -> "dsa", "aa" -> 4.0, "dfsa" -> Seq("a", 3, 4.0))
    //val y: Any = Seq("das", 41, 454, 41.8, "psa", true)
    //val z: Any = Seq(1,2,3,4,5,6,7,8,9)
    //val w: Any = Map("a" -> "dsa", "dfsa" -> "dsa")
    //checkType(x)
    //checkType(y)
    //checkType(z)
    //checkType(w)
  }

  def checkType(a: Any) =
    a match {
      case _: Map[String, Any] => println("Map String to Any")
      case _: Seq[Any] => println("Seq of Any")
      case _: String => println("String")
      case _: Int => println("Int")
      case _: Double => println("Double")
      case _ => println("weird type")
    }

}
