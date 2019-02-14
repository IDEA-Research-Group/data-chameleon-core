package es.us.idea.adt.data.chameleon.spark.conversor

import es.us.idea.adt.data.chameleon.data.complex.{ArrayType, StructType}
import es.us.idea.adt.data.chameleon.data.simple._
import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}
import es.us.idea.adt.data.chameleon.spark
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types

object SparkTypeConversor {

  //val equivalences = Map(
  //  Struct ->
  //
  //)

  def spark2Chameleon(sch: types.DataType): DataType = {
    sch match {
      // StructType está compuesto de varios fields. Todos ellos son de tipo Struct Field.
      // Hay que ćrear un objeto de tipo Complex
      case struct: types.StructType => new StructType(struct.fields.map(f => new Attribute(f.name, spark2Chameleon(f.dataType))) : _*)
      //case sf: StructField => new complex.StructType(new Attribute("hola", new simple.IntegerType()))
      case array: types.ArrayType => new ArrayType(spark2Chameleon(array.elementType))
      case _: types.StringType => new StringType()
      case _: types.IntegerType => new IntegerType()
      case _: types.LongType => new LongType()
      case _: types.DoubleType => new DoubleType()
      case _: types.FloatType => new FloatType()
      case _: types.BooleanType => new BooleanType()
      case _: types.DateType => new DateType()
      case any => throw new Exception(s"Data type could not be converted from Spark to Chameleon: $any")
    }
  }

  def chameleon2Spark(dataType: DataType): types.DataType = {
    dataType match {
      case struct: StructType => DataTypes.createStructType(struct.getAttributes.map(a => DataTypes.createStructField(a.getName, chameleon2Spark(a.getDataType), true)).toArray)
      case array: ArrayType => DataTypes.createArrayType(chameleon2Spark(array.getElementDataType))
      case _: StringType => DataTypes.StringType
      case _: IntegerType => DataTypes.IntegerType
      case _: LongType => DataTypes.LongType
      case _: DoubleType => DataTypes.DoubleType
      case _: FloatType => DataTypes.FloatType
      case _: BooleanType => DataTypes.BooleanType
      case _: DateType => DataTypes.DateType
      case any => throw new Exception(s"Data type could not be converted from chameleon to Spark: $any")
    }
  }

  def findMinSetOfPaths(inPaths: Seq[String]) = {

    val inPathsSplit = inPaths.filter(_.nonEmpty).map(_.split('.'))

    inPathsSplit.map(s => {
      if(s.length > 0) s.head
      else s.mkString(".")
    }).distinct

  }


}
