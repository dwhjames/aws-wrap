import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

    val appName         = "dynamodb-recipes"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      "aws" %% "dynamodb" % "0.1-SNAPSHOT" exclude("play", "play_2.10")
    )

    val main = play.Project(appName, appVersion, appDependencies).settings(
      resolvers += Resolver.url("Local ivy2 Repository", url("file://" + Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
    )

}
