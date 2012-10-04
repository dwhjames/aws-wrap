package aws.simpledb

object Parsers {
  type Domain = String
  import scala.xml.Elem

  def domainsParsers(xml: Elem): Seq[Domain] = (xml \\ "DomainName").map(_.text)
}