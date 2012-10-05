package aws.core

object Parsers {
  import scala.xml.Elem
  def errorsParser(xml: Elem): Seq[(String, String)] = (xml \\ "Error").map { node =>
    (node \ "Code" text) -> (node \ "Message" text)
  }
}