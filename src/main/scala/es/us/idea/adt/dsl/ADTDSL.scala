package es.us.idea.adt.dsl

object ADTDSL {
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




  def recursiveGetValueFromPath(path: String, map: Map[String, Any]): Option[Any] = {
    //path.

    //if(path.contains("."))


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


  def main(args: Array[String]) = {

    println(recursiveGetValueFromPath("consumo.*", inTipo))

  }




}
