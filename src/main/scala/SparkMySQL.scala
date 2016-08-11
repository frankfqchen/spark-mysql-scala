package jdbc

import java.sql.DriverManager
import java.sql.Connection

import scalax.file.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, _}

import scala.collection.mutable.ArrayBuffer


object SparkMySQL {
  val username = "root"
  val password = "root"
  val db = "my_awesome_db"
  val table = "test_json"
  val field = "json"
  val url = "jdbc:mysql://localhost"
  var localOutputFile = "prepared_mysql_data"
  var sc: SparkContext = null
  var connection: Connection = null
  var startTime: DateTime  = null
  var endTime: DateTime  = null
  val numOfRecordsToCreate = 1000000

  def main(args: Array[String]): Unit = {
    setupSpark
    createDB

    setupConnection

    createTable
    insertData
    cleanUp

    readData

    printTimeTake
  }

  def setupSpark(): Unit = {
    var conf = new SparkConf()
    conf.setAppName("Sexy Boom Thang")
      .setMaster("local")
      .set("spark.hadoop.validateOutputSpecs", "false")

    sc = new SparkContext(conf)
  }

  def createDB(): Unit = {
    val connection = DriverManager.getConnection(url, username, password)
    connection.createStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS ${db}")
    connection.close
  }

  def setupConnection(): Unit = {
    connection = DriverManager.getConnection(s"${url}/${db}", username, password)
  }

  def createTable(): Unit = {
    connection.createStatement.executeUpdate(
      s""" CREATE TABLE IF NOT EXISTS ${table} (
      |   id INTEGER NOT NULL AUTO_INCREMENT,
      |   ${field} TEXT,
      |   PRIMARY KEY ( id )
      |   );
      |   """.stripMargin
    )
  }

  def createRandomJson(limit: Int): List[String] = {
    val exampleJson = """{"jkjkjkjkjkkj": ["jkakjsd/asdasd/adadasda"], "iiuiuiuiuiuiu": "2shhwrjkhwekjiiuuihhiuiuhhiuhiuiuh", "hjkashjkdasd": [{"iiiier": "jjkjks/sdfsdf/rwr", "shjhjhjhjtars": 3}]}"""
    List.fill(limit)(exampleJson)
  }

  def insertData(): Unit = {
    val data = createRandomJson(numOfRecordsToCreate)

    // start timing...
    startTime = DateTime.now

    val collection = sc.parallelize(data)
    collection.saveAsTextFile(localOutputFile)

    connection.createStatement.executeUpdate(
      s"""LOAD DATA LOCAL INFILE '${localOutputFile}/part-00000' INTO TABLE ${table} (${field})"""
    )

    // stop timing...
    endTime = DateTime.now
  }

  def readData(): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Sexy Boom Thang").getOrCreate()
    val df = sparkSession.read.format("jdbc")
      .option("url", s"${url}/${db}")
      .option("user", username)
      .option("password", password)
      .option("dbtable", table)

    var readResults = new ArrayBuffer[String]()

    for(limit <- List[Int](100, 1000, 100000, 1000000)) {
      val readStartTime = DateTime.now

      val result = df.load.limit(limit).collect()

      val readEndTime = DateTime.now

      readResults += s"Time Taken : ${(readEndTime.getMillis - readStartTime.getMillis)/1000} seconds to read ${result.size} records"
    }

    readResults.foreach(println)

    sparkSession.stop
  }

  def cleanUp(): Unit = {
    sc.stop
    connection.close
    Path.fromString(s"./${localOutputFile}").deleteRecursively(continueOnFailure = true)
  }

  def printTimeTake(): Unit = {
    println("*"*100)
    println(s"Time taken : ${(endTime.getMillis - startTime.getMillis)/1000} seconds to insert ${numOfRecordsToCreate} records")
    println("*"*100)
  }
}