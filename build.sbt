import Dependencies._

ThisBuild / scalaVersion     := "2.13.2"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.ariskk"
ThisBuild / organizationName := "ariskk"

lazy val zioVersion = "1.0.1"
lazy val zioDeps = Seq(
  "dev.zio" %% "zio" % zioVersion
)

lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"

lazy val otherDeps = Seq(shapeless)

lazy val testDepds = Seq(
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  scalaTest % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "zio-raft",
    libraryDependencies ++= zioDeps ++ testDepds ++ otherDeps,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )



// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
