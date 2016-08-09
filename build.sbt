name := "spark-mysql"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1",
  "org.apache.spark" %% "spark-catalyst" % "1.5.0" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2"
)

javaOptions ++= Seq("-Xmx5G", "-XX:MaxPermSize=5G", "-XX:+CMSClassUnloadingEnabled")