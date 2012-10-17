package aws.dynamodb

/**
 * Represents the status of a table
 */
sealed trait Status {
  def status: String
  override def toString = status
}

object Status {
  object CREATING extends Status { override def status = "Creating" }
  object ACTIVE extends Status { override def status = "Active" }
  object DELETING extends Status { override def status = "Deleting" }
  object UPDATING extends Status { override def status = "Updating" }
  def apply(s: String) = s.toLowerCase match {
    case "creating" => CREATING
    case "active" => ACTIVE
    case "deleting" => DELETING
    case "updating" => UPDATING
    case _ => sys.error("Invalid table status: " + s)
  }
}
