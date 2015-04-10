import scala.util.matching.Regex.Match


scalacOptions in (Compile, doc) :=
  Seq(
    "-encoding", "UTF-8",
    "-sourcepath", baseDirectory.value.getAbsolutePath,
    "-doc-source-url", s"https://github.com/dwhjames/aws-wrap/tree/v${version.value}€{FILE_PATH}.scala")


autoAPIMappings := true

apiURL := Some(url("https://dwhjames.github.io/aws-wrap/api/current/"))

apiMappings ++= {
  val builder = Map.newBuilder[sbt.File, sbt.URL]
  val jarFiles = (managedClasspath in Compile).value.files
  jarFiles.filter(file => file.toString.contains("com.amazonaws/aws-java-sdk")).foreach { awsJarFile =>
    builder += awsJarFile -> url("http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/")
  }
  val bootPath = System.getProperty("sun.boot.library.path")
  if (bootPath ne null) {
    builder += file(bootPath + "/rt.jar") -> url("http://docs.oracle.com/javase/6/docs/api/")
  }
  builder.result()
}

lazy val transformJavaDocLinksTask = taskKey[Unit](
  "Transform JavaDoc links - replace #java.io.File with ?java/io/File.html"
)

transformJavaDocLinksTask := {
  val log = streams.value.log
  log.info("Transforming JavaDoc links")
  val t = (target in (Compile, doc)).value
  (t ** "*.html").get.filter(hasJavadocApiLink).foreach { f =>
    log.debug("Transforming " + f)
    val content1 = javadocApiLink.replaceAllIn(IO.read(f), transformJavaDocLinks)
    val content2 = awsJavadocApiLink.replaceAllIn(content1, transformJavaDocLinks)
    IO.write(f, content2)
  }
}

val transformJavaDocLinks: Match => String = m =>
    "href=\"" + m.group(1) + "?" + m.group(2).replace(".", "/") + ".html"

val javadocApiLink = """href=\"(http://docs\.oracle\.com/javase/6/docs/api/index\.html)#([^"]*)""".r

val awsJavadocApiLink = """href=\"(http://docs\.aws\.amazon\.com/AWSJavaSDK/latest/javadoc/index\.html)#([^"]*)""".r

def hasJavadocApiLink(f: File): Boolean = {
  val content = IO.read(f)
  (javadocApiLink findFirstIn content).nonEmpty ||
  (awsJavadocApiLink findFirstIn content).nonEmpty
}

transformJavaDocLinksTask <<= transformJavaDocLinksTask triggeredBy (doc in Compile)
