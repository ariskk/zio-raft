import sbt.Keys._
import sbt._
import xerial.sbt.Sonatype.autoImport._

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
object Publishing {

  lazy val publishSettings = Seq(
    isSnapshot := version.value endsWith "SNAPSHOT",
    credentials += Credentials(Path.userHome / ".ivy2" / ".sonatype_credentials"),
    ThisBuild / publishTo := {
      val nexus = "https://s01.oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    developers := List(
      Developer(
        "ariskk",
        "Kyriakos Aris Koliopoulos",
        "aris@ariskk.com",
        url("http://ariskk.com")
      )
    ),
    sonatypeProfileName := organization.value,
    homepage := Some(url("http://ariskk.com")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/ariskk/zio-raft"),
        "scm:git@github.com:ariskk/zio-raft.git"
      )
    )
  )

}
