// 生产作业的 SBT 定义。本地验证不需要 SBT / JVM 环境。
//
// 构建: sbt assembly      （需要 JDK 17 + sbt 1.9+）
// 提交: flink run -c streamforge.IngestJob target/scala-3.3.3/streamforge.jar

ThisBuild / scalaVersion := "3.3.3"
ThisBuild / organization := "internal.streamforge"

val flinkVersion = "1.19.0"
val icebergVersion = "1.6.1"

lazy val root = (project in file("."))
  .settings(
    name := "streamforge",
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-streaming-java" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-connector-kafka" % "3.2.0-1.19",
      "org.apache.iceberg" %% "iceberg-flink-runtime-1.19" % icebergVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2",
      "org.slf4j" % "slf4j-simple" % "2.0.13"
    ),
    assembly / assemblyJarName := "streamforge.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )
