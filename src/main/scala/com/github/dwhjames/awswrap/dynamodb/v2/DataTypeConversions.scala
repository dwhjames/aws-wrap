package com.github.dwhjames.awswrap.dynamodb.v2

import com.github.dwhjames.awswrap.dynamodb.{AttributeValueConversionException, QueryBuilder}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
  * Implicit converters for DynamoDB v2 schema.
  *
  * Supports the new data types of BOOL, List and Map
  */
object DataTypeConversions extends QueryBuilder {


  /* Convert Strings to a string AttributeValue and back again */
  implicit def stringSetToAttributeValue = (x: Set[String]) => new AttributeValue().withSS(x.asJavaCollection)

  implicit def attributeValueToString = (x: AttributeValue) => catchAndRethrowConversion { x.getS }
  implicit def attributeValueToStringSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.toSet }
  implicit def attributeValueListToListOfStrings = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToString(_) ) }
  implicit def attributeValueToListOfStrings = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfStrings( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfStrings(x: AttributeValue): Map[String, String] = catchAndRethrowConversion {  x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToString(value))} }


  /* Convert Ints to a numeric AttributeValue and back again */
  implicit def intSetToAttributeValue = (x: Set[Int]) => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToInt    = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toInt }
  implicit def attributeValueToIntSet    = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toInt).toSet }
  implicit def attributeValueListToListOfInts = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToInt(_) ) }
  implicit def attributeValueToListOfInts = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfInts( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfInts(x: AttributeValue): Map[String, Int] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToInt(value))} }


  /* Convert BigInts to a string AttributeValue and back again */
  implicit def bigIntSetToAttributeValue = (x: Set[BigInt]) => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToBigInt = (x: AttributeValue) => catchAndRethrowConversion { BigInt(x.getS) }
  implicit def attributeValueToBigIntSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(BigInt(_)).toSet }
  implicit def attributeValueListToListOfBigInts = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToBigInt(_) ) }
  implicit def attributeValueToListOfBigInts = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfBigInts( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfBigInts(x: AttributeValue): Map[String, BigInt] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToBigInt(value))} }


  /* Convert BigDecimals to a numeric AttributeValue and back again */
  implicit def bigDecimalSetToAttributeValue = (x: Set[BigDecimal]) => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToBigDecimal = (x: AttributeValue) => catchAndRethrowConversion { BigDecimal(x.getN) }
  implicit def attributeValueToBigDecimalSet = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(BigDecimal(_)).toSet }
  implicit def attributeValueListToListOfBigDecimals = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToBigDecimal(_) ) }
  implicit def attributeValueToListOfBigDecimals = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfBigDecimals( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfBigDecimals(x: AttributeValue): Map[String, BigDecimal] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToBigDecimal(value))} }


  /* Convert Double to a string AttributeValue and back again
   *
   * Java double is a double-precision 64-bit IEEE 754 floating point.
   * DynamoDb Number only supports 38 digits precision, exceeding this will result in an exception.
   * So saving Double as a String in Dynamo to work around limitation.
   */
  implicit def doubleSetToAttributeValue = (x: Set[Double]) => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToDouble = (x: AttributeValue) => catchAndRethrowConversion { x.getS.toDouble }
  implicit def attributeValueToDoubleSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(_.toDouble).toSet }
  implicit def attributeValueListToListOfDoubles = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToDouble(_) ) }
  implicit def attributeValueToListOfDoubles = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfDoubles( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfDoubles(x: AttributeValue): Map[String, Double] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToDouble(value))} }


  /* Convert Float to a numeric AttributeValue and back again */
  implicit def floatSetToAttributeValue = (x: Set[Float]) => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToFloat  = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toFloat }
  implicit def attributeValueToFloatSet  = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toFloat).toSet }
  implicit def attributeValueListToListOfFloats = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToFloat(_) ) }
  implicit def attributeValueToListOfFloats = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfFloats( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfFloats(x: AttributeValue): Map[String, Float] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToFloat(value))} }


  /* Convert Long to a numeric AttributeValue and back again */
  implicit def longSetToAttributeValue   = (x: Set[Long])   => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToLong   = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toLong }
  implicit def attributeValueToLongSet   = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toLong).toSet }
  implicit def attributeValueListToListOfLongs = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToLong(_) ) }
  implicit def attributeValueToListOfLongs = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfLongs( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfLongs(x: AttributeValue): Map[String, Long] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToLong(value))} }


  /* Convert Short to a numeric AttributeValue and back again */
  implicit def shortSetToAttributeValue  = (x: Set[Short])  => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToShort  = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toShort }
  implicit def attributeValueToShortSet  = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toShort).toSet }
  implicit def attributeValueListToListOfShorts = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToShort(_) ) }
  implicit def attributeValueToListOfShorts = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfShorts( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfShorts(x: AttributeValue): Map[String, Short] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToShort(value))} }


  /* Convert Byte to a numeric AttributeValue and back again */
  implicit def byteSetToAttributeValue   = (x: Set[Byte])   => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)

  implicit def attributeValueToByte   = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toByte }
  implicit def attributeValueToByteSet   = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toByte).toSet }
  implicit def attributeValueListToListOfBytes = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToByte(_) ) }
  implicit def attributeValueToListOfBytes = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfBytes( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfBytes(x: AttributeValue): Map[String, Byte] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToByte(value))} }


  /* Convert an Array of Bytes to a binary AttributeValue and back again */
  implicit def byteArrayToAttributeValue = (x: Array[Byte]) => new AttributeValue().withB(java.nio.ByteBuffer.wrap(x))
  implicit def byteArrayIterableToAttributeValue = (x: Iterable[Array[Byte]]) => new AttributeValue().withBS(x.map(java.nio.ByteBuffer.wrap(_)).asJavaCollection)

  implicit def attributeValueToByteArray = (x: AttributeValue) => catchAndRethrowConversion { x.getB.array }
  implicit def attributeValueToByteArraySet = (x: AttributeValue) => catchAndRethrowConversion { x.getBS.asScala.map(_.array).toSet }


  /* Convert Boolean to a numeric AttributeValue and back again
  *
  * No facility to store a Dynamo Set of Booleans so storeing them as a List instead
  */
  implicit def booleanSetToAttributeValue = (x: Set[Boolean]) => new AttributeValue().withL(x.map( anyToAttributeValue(_) ).asJavaCollection)
  implicit def booleanSeqToAttributeValue = (x: Seq[Boolean]) => new AttributeValue().withL(x.map( anyToAttributeValue(_) ).asJavaCollection)

  implicit def attributeValueToBoolean(x: AttributeValue): Boolean = catchAndRethrowConversion { x.getBOOL }
  implicit def attributeValueListToListOfBooleans = (x: List[AttributeValue]) => catchAndRethrowConversion { x.map( attributeValueToBoolean(_) ) }
  implicit def attributeValueToListOfBooleans = (x: AttributeValue) => catchAndRethrowConversion { attributeValueListToListOfBooleans( x.getL.asScala.toList ) }
  implicit def attributeValueToMapOfBooleans(x: AttributeValue): Map[String, Boolean] = catchAndRethrowConversion { x.getM.asScala.toMap.map {case (key, value) => (key, attributeValueToBoolean(value))} }
  implicit def attributeValueToBooleanSet(x: AttributeValue) = attributeValueToListOfBooleans(x).toSet


  implicit def anyToAttributeValue(x: Any):AttributeValue = {
    x match {

        case a: String => new AttributeValue().withS(a)
        case z: Boolean => new AttributeValue().withBOOL(z)
        case b: Byte =>  new AttributeValue().withN(b.toString)
        case s: Short => new AttributeValue().withN(s.toString)
        case i: Int => new AttributeValue().withN(i.toString)
        case j: Long => new AttributeValue().withN(j.toString)
        case f: Float => new AttributeValue().withN(f.toString)
        case d: Double => new AttributeValue().withS(d.toString)
        case g: BigInt => new AttributeValue().withS(g.toString)
        case h: BigDecimal => new AttributeValue().withN(h.toString)

        case _ => throw new UnsupportedOperationException(s"Do not know how to convert ${x.toString} to a AttributeValue")
      }
  }

  implicit def anySeqToAttributeValue(x: Seq[_]) =
    new AttributeValue().withL(x.map(anyToAttributeValue(_)).asJavaCollection)

  implicit def mapOfAnyValsToAttributeValue(x: Map[String, _]): AttributeValue =
    new AttributeValue().withM( x.map {case (key, value) => (key, anyToAttributeValue(value)) }.asJava )

  private def catchAndRethrowConversion[T](x: => T): T =
    try {
      x
    } catch {
      case ex: NullPointerException =>
        throw new AttributeValueConversionException(s"RichAttributeValue.as: AttributeValue was not of the expected sort. ${x}")
      case ex: NumberFormatException =>
        throw new AttributeValueConversionException(s"RichAttributeValue.as: the numeric string can't be parsed as the expected numeric type. $x")
    }
}
