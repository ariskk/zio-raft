import sbtrelease.ReleaseStateTransformations._

// TODO Add GitHub BOT to automatically upgrade the version
lazy val scala212   = "2.12.14"
lazy val scala213   = "2.13.7"
lazy val zioVersion = "1.0.13"

inThisBuild(
  List(
    scalaVersion := scala213,
    version := "0.2.0-SNAPSHOT",
    organization := "com.ariskk",
    developers := List(
      Developer(
        "ariskk",
        "Kyriakos Aris Koliopoulos",
        "aris@ariskk.com",
        url("http://ariskk.com")
      )
    )
  )
)

lazy val zioDeps = Seq(
  "dev.zio" %% "zio" % zioVersion
)

lazy val rocksDBVersion = "6.11.4"
lazy val rocksDbDeps = Seq(
  "org.rocksdb" % "rocksdbjni" % rocksDBVersion
)

lazy val testDepds = Seq(
  "dev.zio"       %% "zio-test"     % zioVersion % Test,
  "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
  "org.scalatest" %% "scalatest"    % "3.2.0"    % Test
)

lazy val otherDepds = Seq(
  "com.chuusai" %% "shapeless" % "2.3.9",
  "com.twitter" %% "chill"     % "0.9.5"
)

lazy val CompileTest = "compile->compile;test->test"

lazy val core = (project in file("core")).settings(
  Publishing.publishSettings,
  name := "zio-raft",
)

lazy val rocksdb = (project in file("rocksdb"))
  .settings(
    libraryDependencies ++= rocksDbDeps,
    publish / skip := true,
    name := "zio-raft-rocksdb",
    Publishing.publishSettings
  )
  .dependsOn(core % CompileTest)

lazy val server = (project in file("server"))
  .settings(
    libraryDependencies += "dev.zio" %% "zio-nio" % "1.0.0-RC12",
    publish / skip := true,
    name := "zio-raft-server",
    Publishing.publishSettings
  )
  .dependsOn(
    core    % CompileTest,
    rocksdb % CompileTest
  )

lazy val root = (project in file("."))
  .settings(commands ++= Commands.value)
  .settings(
    ThisBuild / libraryDependencies ++= zioDeps ++ testDepds ++ otherDepds,
    crossScalaVersions := List(scala213, scala212),
    ThisBuild / testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    ThisBuild / scalacOptions ++= List(
      "-Wunused:imports"
    ),
    publish / skip := true,
    ThisBuild / scalafixDependencies += "com.nequissimus" %% "sort-imports" % "0.6.1",
    releaseIgnoreUntrackedFiles := true,
    releaseTagName := (ThisBuild / version).value,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      releaseStepCommandAndRemaining("+compile"),
      releaseStepCommandAndRemaining("test"),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publishSigned"),
      setNextVersion,
      commitNextVersion,
      pushChanges
    ),
    addCompilerPlugin(scalafixSemanticdb),
    ThisBuild / semanticdbEnabled := true,
    ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
  )
  .aggregate(core, rocksdb, server)
