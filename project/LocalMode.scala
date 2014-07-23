import sbt._

object LocalMode {
  val localMode = settingKey[Boolean]("Run integration tests locally")
}
