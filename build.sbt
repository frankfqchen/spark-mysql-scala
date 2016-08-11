name := "spark-mysql"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.12",
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2",
  "joda-time" % "joda-time" % "2.9.4"
)

javaOptions ++= Seq("-Xmx5G", "-XX:MaxPermSize=5G", "-XX:+CMSClassUnloadingEnabled")