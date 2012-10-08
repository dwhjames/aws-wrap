package aws.simpledb

import scala.util.{ Try, Success, Failure }
import play.api.libs.ws.Response
import aws.core.parsers._
import aws.core.parsers.Parser.HandleError

object SDBParsers {
  import scala.xml.Elem

  implicit def domainsParser = HandleError[Seq[SDBDomain]] { r: Response =>
    Success((r.xml \\ "DomainName").map(node => SDBDomain(node.text)))
  }

  implicit def attributesParser = HandleError[Seq[SDBAttribute]] { r: Response =>
    Success((r.xml \\ "Attribute").map { node =>
      SDBAttribute(node \ "Name" text, node \ "Value" text)
    })
  }

  implicit def domainMetadataParser = HandleError[SDBDomainMetadata] { r: Response =>
    val xml = r.xml
    Success(SDBDomainMetadata(
      new java.util.Date((xml \\ "Timestamp" text).toLong * 1000),
      (xml \\ "ItemCount").text.toLong,
      (xml \\ "AttributeValueCount").text.toLong,
      (xml \\ "AttributeNameCount").text.toLong,
      (xml \\ "ItemNamesSizeBytes").text.toLong,
      (xml \\ "AttributeValuesSizeBytes").text.toLong,
      (xml \\ "AttributeNamesSizeBytes").text.toLong
    ))
  }

  implicit def itemParser = HandleError[Seq[SDBItem]] { r: Response =>
    Success((r.xml \\ "Item").map { node =>
      SDBItem(
        node \ "Name" text,
        node \ "Attribute" map { attrNode =>
          SDBAttribute(attrNode \ "Name" text, attrNode \ "Value" text)
        }
      )
    })
  }

}
