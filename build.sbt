// Project name (artifact name in Maven)
name := "catrees"

// orgnization name (e.g., the package name of the project)
organization := "me.winsh"

version := "0.1"

// project description
description := """Implementations of Contention Adapting Search Trees"""

// Enables publishing to maven repo
publishMavenStyle := true

// POM settings for Sonatype
homepage := Some(url("https://github.com/kjellwinblad/ca_trees_java"))

scmInfo := Some(ScmInfo(url("https://github.com/kjellwinblad/ca_trees_java"),
                            "git@github.com:kjellwinblad/ca_trees_java.git"))

developers := List(Developer("kjellwinblad",
                             "Kjell Winblad",
                             "kjellwinblad@gmail.com",
                             url("https://winsh.me")))

licenses += ("GNU General Public License (GPL) Version 3", url("https://www.gnu.org/licenses/gpl-3.0.en.html"))

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// library dependencies. (orginization name) % (project name) % (version)


//Test dependencies

scalaVersion := "2.11.1"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.1.7" % "test"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.1.7" % "test"

libraryDependencies += "org.scala-lang" % "scala-actors" % "2.11.1" % "test"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.1" % "test"




