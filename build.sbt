inThisBuild(
  List(
    scalaVersion := "2.13.2",
    version := "0.1.0-SNAPSHOT",
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

lazy val zioVersion = "1.0.1"
lazy val zioDeps = Seq(
  "dev.zio" %% "zio" % zioVersion
)

lazy val testDepds = Seq(
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "zio-raft",
    libraryDependencies ++= zioDeps ++ testDepds,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    scalacOptions ++= List(
      "-Wunused:imports"
    ),
    addCompilerPlugin(scalafixSemanticdb)
  )
