package aws.simpledb

import aws.core.parsers._

object SDBParsers {
  import scala.xml.Elem

  implicit def domainsParser = Parser[Seq[SDBDomain]] { xml: Elem =>
    (xml \\ "DomainName").map(node => SDBDomain(node.text))
  }

  implicit def attributesParser = Parser[Seq[SDBAttribute]] { xml: Elem =>
    (xml \\ "Attribute").map { node =>
      SDBAttribute(node \ "Name" text, node \ "Value" text)
    }
  }

  implicit def domainMetadataParser = Parser[SDBDomainMetadata] { xml: Elem =>
    SDBDomainMetadata(
      new java.util.Date((xml \\ "Timestamp" text).toLong * 1000),
      (xml \\ "ItemCount").text.toLong,
      (xml \\ "AttributeValueCount").text.toLong,
      (xml \\ "AttributeNameCount").text.toLong,
      (xml \\ "ItemNamesSizeBytes").text.toLong,
      (xml \\ "AttributeValuesSizeBytes").text.toLong,
      (xml \\ "AttributeNamesSizeBytes").text.toLong
    )
  }

  implicit def itemParser = Parser[Seq[SDBItem]] { xml: Elem =>
    (xml \\ "Item").map { node =>
      SDBItem(
        node \ "Name" text,
        node \ "Attribute" map { attrNode =>
          SDBAttribute(attrNode \ "Name" text, attrNode \ "Value" text)
        }
      )
    }
  }

}
