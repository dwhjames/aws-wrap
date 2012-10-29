package aws.core

package object signature {

  def canonicalQueryString(params: Seq[(String, String)]) =
    params.sortBy(_._1).map { p => SignerEncoder.encode(p._1) + "=" + SignerEncoder.encode(p._2) }.mkString("&")

}
