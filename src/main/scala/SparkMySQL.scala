package jdbc

import java.sql.DriverManager
import java.sql.Connection
import scalax.file.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import com.datastax.spark.connector._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.parsing.json._
import org.joda.time.{DateTime, _}


object SparkMySQL {
  val username = "root"
  val password = ""
  val db = "my_awesome_db"
  val table = "test_json"
  val field = "json"
  val url = "jdbc:mysql://localhost"
  var localOutputFile = "prepared_mysql_data"
  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    setupSpark
    createDB
    createTable
    sparkWriteDataToDisk
    insertData
    cleanUp


    // close connection
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
  }

  def createTable(): Unit = {
    val connection = DriverManager.getConnection(s"${url}/${db}", username, password)

    connection.createStatement.executeUpdate(
      s""" CREATE TABLE IF NOT EXISTS ${table} (
      |   id INTEGER NOT NULL AUTO_INCREMENT,
      |   ${field} TEXT,
      |   PRIMARY KEY ( id )
      |   )
      |   """.stripMargin
    )
  }

  def createRandomJson(limit: Int): List[String] = {
    val exampleJson = """{"jkjkjkjkjkkj": ["jkakjsd/asdasd/adadasda"], "iiuiuiuiuiuiu": "2shhwrjkhwekjiiuuihhiuiuhhiuhiuiuh", "hjkashjkdasd": [{"iiiier": "jjkjks/sdfsdf/rwr", "shjhjhjhjtars": 3}]}"""
    List.fill(limit)(exampleJson)
  }

  def sparkWriteDataToDisk(): Unit = {
    val collection = sc.parallelize(createRandomJson(1000000))
    collection.saveAsTextFile(localOutputFile)
  }

  def insertData(): Unit = {
    val connection = DriverManager.getConnection(s"${url}/${db}", username, password)

    connection.createStatement.executeUpdate(
      s"""LOAD DATA LOCAL INFILE '${localOutputFile}/part-00000' INTO TABLE ${table} (${field})"""
    )
  }

  def cleanUp(): Unit = {
    sc.stop
    Path.fromString(s"./${localOutputFile}").deleteRecursively(continueOnFailure = true)
  }
}