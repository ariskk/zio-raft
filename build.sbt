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
  "com.chuusai" %% "shapeless" % "2.4.0-M1",
  "com.twitter" %% "chill" % "0.9.5"
)

lazy val core = (project in file("core"))

lazy val rocksdb = (project in file("rocksdb")).settings(
  libraryDependencies ++= rocksDbDeps
).dependsOn(core % "compile->compile;test->test")

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
  .aggregate(core, rocksdb)
