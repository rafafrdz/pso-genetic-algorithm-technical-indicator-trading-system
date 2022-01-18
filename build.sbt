name := "mf-dabi-pso-technical-indicator-trading-system"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion withSources() withJavadoc()
libraryDependencies += "org.scalanlp" %% "breeze" % "1.0"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.0"