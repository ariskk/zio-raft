import Dependencies._

ThisBuild / scalaVersion     := "2.13.2"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.ariskk"
ThisBuild / organizationName := "ariskk"

lazy val zioVersion = "1.0.1"
lazy val zioDeps = Seq(
  "dev.zio" %% "zio" % zioVersion
)

lazy val testDepds = Seq(
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  scalaTest % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "zio-raft",
    libraryDependencies ++= zioDeps ++ testDepds,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
