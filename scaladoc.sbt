import scala.util.matching.Regex.Match


scalacOptions in (Compile, doc) ++=
  Seq(
    "-sourcepath", baseDirectory.value.getAbsolutePath,
    "-doc-source-url", s"https://github.com/pellucidanalytics/aws-wrap/tree/v${version.value}€{FILE_PATH}.scala")


autoAPIMappings := true

apiURL := Some(url("https://pellucidanalytics.github.io/aws-wrap/api/current/"))

apiMappings += {
  val jarFiles = (managedClasspath in Compile).value.files
  val datomicJarFile = jarFiles.find(file => file.toString.contains("com.amazonaws/aws-java-sdk")).get
  (datomicJarFile -> url("http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/"))
}

lazy val transformJavaDocLinksTask = taskKey[Unit](
  "Transform JavaDoc links - replace #java.io.File with ?java/io/File.html"
)

transformJavaDocLinksTask := {
  val log = streams.value.log
  log.info("Transforming JavaDoc links")
  val t = (target in (Compile, doc)).value
  (t ** "*.html").get.filter(hasJavadocApiLink).foreach { f =>
    log.info("Transforming " + f)
    val newContent = javadocApiLink.replaceAllIn(IO.read(f), transformJavaDocLinks)
    IO.write(f, newContent)
  }
}

val transformJavaDocLinks: Match => String = m =>
    "href=\"" + m.group(1) + "?" + m.group(2).replace(".", "/") + ".html"

val javadocApiLink = """href=\"(http://docs\.aws\.amazon\.com/AWSJavaSDK/latest/javadoc/index\.html)#([^"]*)""".r

def hasJavadocApiLink(f: File): Boolean = (javadocApiLink findFirstIn IO.read(f)).nonEmpty

transformJavaDocLinksTask <<= transformJavaDocLinksTask triggeredBy (doc in Compile)
