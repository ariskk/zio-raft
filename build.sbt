inThisBuild(
  List(
    scalaVersion := "2.13.7",
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

lazy val zioVersion = "1.0.13"
lazy val zioDeps = Seq(
  "dev.zio" %% "zio" % zioVersion
)

lazy val rocksDBVersion = "6.11.4"
lazy val rocksDbDeps = Seq(
  "org.rocksdb" % "rocksdbjni" % rocksDBVersion
)

lazy val testDepds = Seq(
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)

lazy val otherDepds = Seq(
  "com.chuusai" %% "shapeless" % "2.3.9",
  "com.twitter" %% "chill" % "0.9.5"
)

lazy val CompileTest = "compile->compile;test->test"

lazy val core = project in file("core")

lazy val rocksdb = (project in file("rocksdb")).settings(
  libraryDependencies ++= rocksDbDeps
).dependsOn(core % CompileTest)

lazy val server = (project in file("server")).settings(
  libraryDependencies +=  "dev.zio" %% "zio-nio" % "1.0.0-RC12"
).dependsOn(
  core % CompileTest,
  rocksdb % CompileTest
)

lazy val root = (project in file("."))
  .settings(
    name := "zio-raft",
    libraryDependencies in ThisBuild ++= zioDeps ++ testDepds ++ otherDepds,
    testFrameworks in ThisBuild += new TestFramework("zio.test.sbt.ZTestFramework"),
    scalacOptions in ThisBuild ++= List(
      "-Wunused:imports"
    ),
    addCompilerPlugin(scalafixSemanticdb),
    semanticdbEnabled in ThisBuild := true,
    semanticdbVersion in ThisBuild := scalafixSemanticdb.revision
  )
  .aggregate(core, rocksdb, server)
