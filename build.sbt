// Project name (artifact name in Maven)
name := "catrees"

// orgnization name (e.g., the package name of the project)
organization := "me.winsh"

version := "0.1-SNAPSHOT"

// project description
description := """Implementations of Contention Adapting Search Trees"""

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(

)
