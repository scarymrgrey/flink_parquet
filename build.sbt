ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "flink-batching"

version := "0.1"

organization := "org.grid"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.10.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-parquet" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-hadoop-compatibility" % flinkVersion % "provided",
  "org.apache.flink" % "flink-avro" % "1.10.1",
  "org.apache.parquet" % "parquet-avro" % "1.11.0",
  "com.github.mjakubowski84" %% "parquet4s-core" % "1.0.0",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("org.grid.Job")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
