/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dwhjames.awswrap

import scala.collection.JavaConverters._

package object dynamodb extends QueryBuilder {

  /** String to a string AttributeValue */
  implicit val stringToAttributeValue = (x: String) => new AttributeValue().withS(x)
  /** String collection to a string set AttributeValue */
  implicit val stringIterableToAttributeValue = (x: Iterable[String]) => new AttributeValue().withSS(x.asJavaCollection)


  /** Double to a numeric AttributeValue */
  implicit val doubleToAttributeValue     = (x: Double)     => new AttributeValue().withN(x.toString)
  /** Float to a numeric AttributeValue */
  implicit val floatToAttributeValue      = (x: Float)      => new AttributeValue().withN(x.toString)
  /** Long to a numeric AttributeValue */
  implicit val longToAttributeValue       = (x: Long)       => new AttributeValue().withN(x.toString)
  /** Int to a numeric AttributeValue */
  implicit val intToAttributeValue        = (x: Int)        => new AttributeValue().withN(x.toString)
  /** Short to a numeric AttributeValue */
  implicit val shortToAttributeValue      = (x: Short)      => new AttributeValue().withN(x.toString)
  /** Byte to a numeric AttributeValue */
  implicit val byteToAttributeValue       = (x: Byte)       => new AttributeValue().withN(x.toString)
  /** BigDecimal to a numeric AttributeValue */
  implicit val bigDecimalToAttributeValue = (x: BigDecimal) => new AttributeValue().withN(x.toString)


  /** Double collection to a numeric set AttributeValue */
  implicit val doubleIterableToAttributeValue = (x: Iterable[Double]) => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  /** Float collection to a numeric set AttributeValue */
  implicit val floatIterableToAttributeValue  = (x: Iterable[Float])  => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  /** Long collection to a numeric set AttributeValue */
  implicit val longIterableToAttributeValue   = (x: Iterable[Long])   => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  /** Int collection to a numeric set AttributeValue */
  implicit val intIterableToAttributeValue    = (x: Iterable[Int])    => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  /** Short collection to a numeric set AttributeValue */
  implicit val shortIterableToAttributeValue  = (x: Iterable[Short])  => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  /** Byte collection to a numeric set AttributeValue */
  implicit val byteIterableToAttributeValue   = (x: Iterable[Byte])   => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)


  /** Array[Byte] to a binary AttributeValue */
  implicit val byteArrayToAttributeValue = (x: Array[Byte]) => new AttributeValue().withB(java.nio.ByteBuffer.wrap(x))
  /** Array[Byte] collection to a binary set AttributeValue */
  implicit val byteArrayIterableToAttributeValue = (x: Iterable[Array[Byte]]) => new AttributeValue().withBS(x.map(java.nio.ByteBuffer.wrap(_)).asJavaCollection)


  /** Boolean to a string AttributeValue */
  implicit val booleanToAttributeValue = (x: Boolean) => new AttributeValue().withS(x.toString)
  /** Boolean collection to a string set AttributeValue */
  implicit val booleanIterableToAttributeValue = (x: Iterable[Boolean]) => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)

  /** BigInt to a string AttributeValue */
  implicit val bigIntToAttributeValue     = (x: BigInt)     => new AttributeValue().withS(x.toString)

  /** BigInt to a string set AttributeValue */
  implicit val bigIntIterableToAttributeValue     = (x: Iterable[BigInt])     => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)
  /** BigDecimal to a string set AttributeValue */
  implicit val bigDecimalIterableToAttributeValue = (x: Iterable[BigDecimal]) => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)

  /**
    * An enrichment of the
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html AttributeValue]]
    * class.
    *
    * An implicit class to extend attribute values with a type-safe(ish)
    * casting method.
    *
    * @param attrVal
    *     the underlying AttributeValue.
    */
  implicit class RichAttributeValue(attrVal: AttributeValue) {

    /**
      * Casts an attribute value to a convertable type.
      *
      * A type-safe(ish) cast from an attribute value,
      * using an implicit view.
      *
      * @tparam T
      *     the type of object to cast.
      * @param conv
      *     the implicit view.
      * @return the converted value.
      * @throws AttributeValueConversionException
      *     if the conversion cannot be made.
      */
    def as[T](implicit conv: AttributeValue => T): T =
      catchAndRethrowConversion(conv(attrVal))
  }

  private def catchAndRethrowConversion[T](x: => T): T =
    try {
      x
    } catch {
      case ex: NullPointerException =>
          throw new AttributeValueConversionException("RichAttributeValue.as: AttributeValue was not of the expected sort.")
      case ex: NumberFormatException =>
        throw new AttributeValueConversionException("RichAttributeValue.as: the numeric string can't be parsed as the expected numeric type.")
    }

  /** string AttributeValue to String */
  implicit val attributeValueToString = (x: AttributeValue) => catchAndRethrowConversion { x.getS }
  /** string set AttributeValue to Set[String] */
  implicit val attributeValueToStringSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.toSet }

  /** numeric AttributeValue to Double */
  implicit val attributeValueToDouble = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toDouble }
  /** numeric AttributeValue to Float */
  implicit val attributeValueToFloat  = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toFloat }
  /** numeric AttributeValue to Long */
  implicit val attributeValueToLong   = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toLong }
  /** numeric AttributeValue to Int */
  implicit val attributeValueToInt    = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toInt }
  /** numeric AttributeValue to Short */
  implicit val attributeValueToShort  = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toShort }
  /** numeric AttributeValue to Byte */
  implicit val attributeValueToByte   = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toByte }

  /** numeric set AttributeValue to Set[Double] */
  implicit val attributeValueToDoubleSet = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toDouble).toSet }
  /** numeric set AttributeValue to Set[Float] */
  implicit val attributeValueToFloatSet  = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toFloat).toSet }
  /** numeric set AttributeValue to Set[Long] */
  implicit val attributeValueToLongSet   = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toLong).toSet }
  /** numeric set AttributeValue to Set[Int] */
  implicit val attributeValueToIntSet    = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toInt).toSet }
  /** numeric set AttributeValue to Set[Short] */
  implicit val attributeValueToShortSet  = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toShort).toSet }
  /** numeric set AttributeValue to Set[Byte] */
  implicit val attributeValueToByteSet   = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toByte).toSet }

  /** binary AttributeValue to Array[Byte] */
  implicit val attributeValueToByteArray = (x: AttributeValue) => catchAndRethrowConversion { x.getB.array }
  /** binary set AttributeValue to Set[Array[Byte]] */
  implicit val attributeValueToByteArraySet = (x: AttributeValue) => catchAndRethrowConversion { x.getBS.asScala.map(_.array).toSet }

  /** string AttributeValue to Boolean */
  implicit val attributeValueToBoolean = (x: AttributeValue) => catchAndRethrowConversion { x.getS.toBoolean }
  /** string set AttributeValue to Set[Boolean] */
  implicit val attributeValueToBooleanSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(_.toBoolean).toSet }

  /** string AttributeValue to BigInt */
  implicit val attributeValueToBigInt     = (x: AttributeValue) => catchAndRethrowConversion { BigInt(x.getS) }
  /** string AttributeValue to BigDecimal */
  implicit val attributeValueToBigDecimal = (x: AttributeValue) => catchAndRethrowConversion { BigDecimal(x.getN) }

  /** string set AttributeValue to Set[BigInt] */
  implicit val attributeValueToBigIntSet     = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(BigInt(_)).toSet }
  /** string set AttributeValue to Set[BigDecimal] */
  implicit val attributeValueToBigDecimalSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(BigDecimal(_)).toSet }

}
