/*
 * Copyright 2012 Pellucid and Zenexity
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

package aws.simpledb

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object SDBParsers {
  import scala.xml.Elem
  import language.postfixOps

  implicit def simpleDBMetaParser = Parser[SimpleDBMeta] { r =>
    Success(SimpleDBMeta(r.xml \\ "RequestId" text, r.xml \\ "BoxUsage" text))
  }

  implicit def domainsParser = Parser[Seq[SDBDomain]] { r: Response =>
    Success((r.xml \\ "DomainName").map(node => SDBDomain(node.text)))
  }

  implicit def attributesParser = Parser[Seq[SDBAttribute]] { r: Response =>
    Success((r.xml \\ "Attribute").map { node =>
      SDBAttribute(node \ "Name" text, node \ "Value" text)
    })
  }

  implicit def domainMetadataParser = Parser[SDBDomainMetadata] { r: Response =>
    val xml = r.xml
    Success(SDBDomainMetadata(
      new java.util.Date((xml \\ "Timestamp" text).toLong * 1000),
      (xml \\ "ItemCount").text.toLong,
      (xml \\ "AttributeValueCount").text.toLong,
      (xml \\ "AttributeNameCount").text.toLong,
      (xml \\ "ItemNamesSizeBytes").text.toLong,
      (xml \\ "AttributeValuesSizeBytes").text.toLong,
      (xml \\ "AttributeNamesSizeBytes").text.toLong))
  }

  implicit def itemParser = Parser[Seq[SDBItem]] { r: Response =>
    Success((r.xml \\ "Item").map { node =>
      SDBItem(
        node \ "Name" text,
        node \ "Attribute" map { attrNode =>
          SDBAttribute(attrNode \ "Name" text, attrNode \ "Value" text)
        })
    })
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SimpleDBMeta, T]] =
    Parser.xmlErrorParser[SimpleDBMeta].or(Parser.resultParser(simpleDBMetaParser, p))

}
