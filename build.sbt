val ZioVersion          = "1.0.0-RC18"
val SparkVersion        = "2.4.3"
val SparkExcelVersion   = "0.13.1"
val PureConfigVersion   = "0.12.0"

lazy val root = (project in file("."))
  .settings(
    organization := "com.vyunsergey",
    name := "census-brands-clusters",
    version := "0.0.1",
    scalaVersion := "2.12.11",
    scalacOptions ++= scalaCompilerOptions,
    libraryDependencies ++= commonLibraryDependencies,
    addCompilerPlugin("org.spire-math" %% "kind-projector"     % "0.9.6"),
    addCompilerPlugin("com.olegpy"     %% "better-monadic-for" % "0.2.4")
  )

lazy val commonLibraryDependencies = Seq(
  //ZIO
  "dev.zio"                    %% "zio"           % ZioVersion,
  "dev.zio"                    %% "zio-test"      % ZioVersion % Test,
  "dev.zio"                    %% "zio-test-sbt"  % ZioVersion % Test,
  //Spark
  "org.apache.spark"           %% "spark-core"    % SparkVersion,
  "org.apache.spark"           %% "spark-sql"     % SparkVersion,
  //Spark Excel
  "com.crealytics"             %% "spark-excel"   % SparkExcelVersion,
  //PureConfig
  "com.github.pureconfig"      %% "pureconfig"    % PureConfigVersion
)

lazy val scalaCompilerOptions = Seq(
  "-deprecation",               // Emit warning and location for usages of deprecated APIs.
  "-encoding", "UTF-8",         // Specify character encoding used by source files.
  "-language:higherKinds",      // Allow higher-kinded types
  "-language:postfixOps",       // Allows operator syntax in postfix position (deprecated since Scala 2.10)
  "-feature",                   // Emit warning and location for usages of features that should be imported explicitly.
  "-Ypartial-unification",      // Enable partial unification in type constructor inference
  "-Xfatal-warnings",           // Fail the compilation if there are any warnings
)
