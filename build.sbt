scalaVersion := "2.13.8"
name         := "embedded-mongo"
organization := "io.github.kirill5k"
version      := "1.0"
libraryDependencies ++= Seq(
  "io.github.kirill5k" %% "mongo4cats-core"           % "0.6.6",
  "io.github.kirill5k" %% "mongo4cats-circe"          % "0.6.6",
  "org.scalatest"      %% "scalatest"                 % "3.2.15",
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "4.4.0"
)
