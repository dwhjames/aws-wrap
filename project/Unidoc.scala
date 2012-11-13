// Borrowed from Akka

import sbt._
import sbt.Keys._
import sbt.Project.Initialize

object Unidoc {
  val unidocDirectory = SettingKey[File]("unidoc-directory")
  val unidocExclude = SettingKey[Seq[String]]("unidoc-exclude")
  val unidocAllSources = TaskKey[Seq[Seq[File]]]("unidoc-all-sources")
  val unidocSources = TaskKey[Seq[File]]("unidoc-sources")
  val unidocSourceDirs = TaskKey[Seq[File]]("unidoc-source-dirs")
  val unidocAllClasspaths = TaskKey[Seq[Classpath]]("unidoc-all-classpaths")
  val unidocClasspath = TaskKey[Seq[File]]("unidoc-classpath")
  val unidoc = TaskKey[File]("unidoc", "Create unified scaladoc for all aggregates")

  lazy val settings = Seq(
    scalaVersion := AWS.scalaVersion,
    unidocDirectory <<= crossTarget / "unidoc",
    unidocExclude := Seq.empty,
    unidocAllSources <<= (thisProjectRef, buildStructure, unidocExclude) flatMap allSources,
    unidocSourceDirs <<= (thisProjectRef, buildStructure, unidocExclude) map sourceDirs,
    unidocSources <<= unidocAllSources map { _.flatten },
    unidocAllClasspaths <<= (thisProjectRef, buildStructure, unidocExclude) flatMap allClasspaths,
    unidocClasspath <<= unidocAllClasspaths map { _.flatten.map(_.data).distinct },
    unidoc <<= unidocTask
  )

  def sourceDirs(projectRef: ProjectRef, structure: Load.BuildStructure, exclude: Seq[String]): Seq[File] = {
    val projects = aggregated(projectRef, structure, exclude)
    val projDirs = projects flatMap { baseDirectory in LocalProject(_) get structure.data }
    projDirs flatMap { d => Seq(d / "src" / "main" / "java", d / "src" / "main" / "scala") }
  }

  def allSources(projectRef: ProjectRef, structure: Load.BuildStructure, exclude: Seq[String]): Task[Seq[Seq[File]]] = {
    val projects = aggregated(projectRef, structure, exclude)
    projects flatMap { sources in Compile in LocalProject(_) get structure.data } join
  }

  def allClasspaths(projectRef: ProjectRef, structure: Load.BuildStructure, exclude: Seq[String]): Task[Seq[Classpath]] = {
    val projects = aggregated(projectRef, structure, exclude)
    projects flatMap { dependencyClasspath in Compile in LocalProject(_) get structure.data } join
  }

  def aggregated(projectRef: ProjectRef, structure: Load.BuildStructure, exclude: Seq[String]): Seq[String] = {
    val aggregate = Project.getProject(projectRef, structure).toSeq.flatMap(_.aggregate)
    aggregate flatMap { ref =>
      if (exclude contains ref.project) Seq.empty
      else ref.project +: aggregated(ref, structure, exclude)
    }
  }

  def unidocTask: Initialize[Task[File]] = {
    (compilers, cacheDirectory, unidocSources, unidocSourceDirs, unidocClasspath, unidocDirectory, scalacOptions in doc, streams) map {
      (compilers, cache, sources, javaDirs, classpath, target, options, s) => {
        // Javadoc
        """javadoc -windowtitle aws -doctitle AWS&nbsp;""" + AWS.version + """&nbsp;Java&nbsp;API  -sourcepath %s -d %s -subpackages com.pellucid -classpath %s""".format(javaDirs.mkString(":"), target / "javadoc", classpath.mkString(":")) ! s.log
        // Scaladoc
        val scaladoc = new Scaladoc(100, compilers.scalac)
        scaladoc.cached(cache / "unidoc", "main", sources.filter(_.getName endsWith ".scala"), classpath, target / "scaladoc", options, s.log)
        target
      }
    }
  }
}
