import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._
import com.rojoma.simplearm.util._
import com.rojoma.json.util.JsonUtil._

object SocrataEvents extends Build {
  def tagVersion(resourceManaged: File, version: String, scalaVersion: String): Seq[File] = {
    val file = resourceManaged / "version"
    val revision = Process(Seq("git", "describe", "--always", "--dirty")).!!.split("\n")(0)

    val result = Map(
      "version" -> version,
      "revision" -> revision,
      "scala" -> scalaVersion
    ) ++ Option(System.getenv("BUILD_TAG")).map("build" -> _)

    resourceManaged.mkdirs()
    for {
      stream <- managed(new java.io.FileOutputStream(file))
      w <- managed(new java.io.OutputStreamWriter(stream, "UTF-8"))
    } {
      writeJson(w, result, pretty = true)
      w.write("\n")
    }

    Seq(file)
  }

  lazy val socrataEvents = Project(
    "socrata-events",
    file("."),
    settings = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ Seq(Keys.parallelExecution := false) ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
      Seq(
      "com.twitter" %% "twitter-server" % "1.3.1",
      "com.socrata" %% "eurybates" % "0.1.2",
      "com.socrata" %% "socrata-zookeeper" % "0.0.1",
      "org.apache.activemq" % "activemq-core" % "5.3.0" % "optional",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "log4j" % "log4j" % "1.2.16"
    ) },
      discoveredMainClasses in Compile := Seq("com.socrata.eventlog.Main"),
      resourceGenerators in Compile <+= (resourceManaged in Compile, version in Compile, scalaVersion in Compile) map tagVersion,
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar"),
    resolvers += "twitter-maven" at "http://maven.twttr.com"
  )
  )
}
