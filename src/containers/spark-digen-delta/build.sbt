val sparkVersion  = sys.props.getOrElse("spark.version", "3.5.8")
val deltaVersion  = sys.props.getOrElse("delta.version", "3.3.2")
val hadoopVersion = sys.props.getOrElse("hadoop.version", "3.3.4")
val scalaVer      = sys.props.getOrElse("scala.version", "2.12.18")

ThisBuild / scalaVersion := scalaVer
ThisBuild / version      := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "datagen",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"    % sparkVersion,
      "io.delta"          %% "delta-spark"  % deltaVersion,
      "com.databricks"    %% "spark-xml"    % "0.18.0",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    },
    assembly / assemblyJarName := "datagen-assembly.jar",
  )
