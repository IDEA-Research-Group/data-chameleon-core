package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.{Attribute, DataType}
import es.us.idea.adt.data.chameleon.data.complex.{ArrayType, StructType}
import es.us.idea.adt.data.chameleon.data.simple._
import org.apache.spark
import org.apache.spark.sql.types.DataTypes

object SparkTypeConversor {

  //val equivalences = Map(
  //  Struct ->
  //
  //)

  def spark2Chameleon(sch: spark.sql.types.DataType): DataType = {
    sch match {
      // StructType está compuesto de varios fields. Todos ellos son de tipo Struct Field.
      // Hay que ćrear un objeto de tipo Complex
      case struct: spark.sql.types.StructType => new StructType(struct.fields.map(f => new Attribute(f.name, spark2Chameleon(f.dataType))) : _*)
      //case sf: StructField => new complex.StructType(new Attribute("hola", new simple.IntegerType()))
      case array: spark.sql.types.ArrayType => new ArrayType(spark2Chameleon(array.elementType))
      case _: spark.sql.types.StringType => new StringType()
      case _: spark.sql.types.IntegerType => new IntegerType()
      case _: spark.sql.types.LongType => new LongType()
      case _: spark.sql.types.DoubleType => new DoubleType()
      case _: spark.sql.types.FloatType => new FloatType()
      case _: spark.sql.types.BooleanType => new BooleanType()
      case _: spark.sql.types.DateType => new DateType()
      case any => throw new Exception(s"Data type could not be converted from Spark to Chameleon: $any")
    }
  }

  def chameleon2Spark(dataType: DataType): spark.sql.types.DataType = {
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



}
