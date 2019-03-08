package es.us.idea.adt.data.chameleon.internal.dtf.udf

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.internal.Evaluable

case class UDF(f: AnyRef, dataType: DataType) {

  // Para permitir pasar los argumentos directamente a la DTF
  def apply(evals: Evaluable*): Evaluable = {

    new UDFOperator(evals, new UDF(f, dataType))
    // Tengo que tener uno unario y otro N-Ario
    //if(evals.size > 0){
    //  else new UnaryUDFOperator(evals.head, new UDF(f, dataType))
    //} else throw new Exception("Nullary operator has not been implemented yet")
  }

  def evaluate(value: Any*): Any = {

    // TODO: Tratar los valores nulos. Introducir un parámetro que indique el criterio de qué hacer en caso de que haya un valor nulo
    def getRawValue(v: Any): Any = {
      v match {
        case Some(a) => getRawValue(a)
        case other => other
        //case _ => throw new Exception("DTF input type not expected")
      }
    }

    // TODO: Meter esto en un try/catch y capturar la IndexOutOfBounds
    val result =
      f match {
        case f0: Function0[Any] => f0()
        case f1: Function1[Any, Any] => f1(getRawValue(value(0)))
        case f2: Function2[Any, Any, Any] => f2(getRawValue(value(0)), getRawValue(value(1)))
        case f3: Function3[Any, Any, Any, Any] => f3(getRawValue(value(0)), getRawValue(value(1)), getRawValue(value(2)))
        case f4: Function4[Any, Any, Any, Any, Any] => f4(getRawValue(value(0)), getRawValue(value(1)), getRawValue(value(2)), getRawValue(value(3)))
      }

    // TODO En otra función, debería comprobar con qué FunctionX hace match
    result

  }

  def getDataType = dataType


  //https://stackoverflow.com/questions/30403120/scala-pattern-matching-on-different-type-of-seq
  // It does not work
  //def evaluate() = {
  //  if(testReturnType[Int]()) new IntegerType()
  //  else if(testReturnType[Double]()) new DoubleType()
  //  else if(testReturnType[Seq[Int]]()) new ArrayType(new IntegerType())
  //  else if(testReturnType[Seq[Double]]()) new ArrayType(new DoubleType())
  //}
  //def testReturnType[T]() = {
  //  f match {
  //    case f0: Function0[T] => true
  //    case f1: Function1[Any, T] => true
  //    case f1: Function2[Any, Any, T] => true
  //    case _ => false
  //  }
  //}
  //def getValue(in: Any): Any = evaluate(in)

  // n-ary

  //def getDataType(dt: DataType): DataType = {
  //  dataType match {
  //    case Some(x) => x
  //    case _ => calculateDT match {
  //      case Some(funct) => funct(dt)
  //      case _ => dt
  //    }
  //  }
  //}

  //// Nullary
  //def getDataType(): DataType = {
  //  dataType match {
  //    case Some(x) => x
  //    case _ => throw new Exception("DataType must be specified in nullary operators")
  //  }
  //}

}

/**
  * Overloaded constructors for this case class
  */
object DTF {

  def apply(f: AnyRef, dataType: DataType) = new UDF(f, dataType)
  //def apply(f: AnyRef, calculateDT: DataType => DataType) = new UDF(f, None, Some(calculateDT))

}