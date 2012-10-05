package aws.simpledb

import aws.core.parsers._

object SDBParsers {
  import scala.xml.Elem

  implicit def domainsParser = Parser[Seq[SDBDomain]] { xml: Elem =>
    (xml \\ "DomainName").map(node => SDBDomain(node.text))
  }

}
