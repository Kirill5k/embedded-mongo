import sbtghactions.JavaSpec

ThisBuild / scalaVersion                        := "2.13.8"
ThisBuild / organization                        := "io.github.kirill5k"
ThisBuild / githubWorkflowPublishTargetBranches := Nil
ThisBuild / githubWorkflowJavaVersions          := Seq(JavaSpec.temurin("19"))
ThisBuild / version                             := "1.0"

name := "embedded-mongo"
libraryDependencies ++= Seq(
  "io.github.kirill5k" %% "mongo4cats-core"           % "0.6.6",
  "io.github.kirill5k" %% "mongo4cats-circe"          % "0.6.6",
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "4.4.0",
  "org.immutables"      % "value"                     % "2.9.2",
  "org.scalatest"      %% "scalatest"                 % "3.2.15" % Test
)
