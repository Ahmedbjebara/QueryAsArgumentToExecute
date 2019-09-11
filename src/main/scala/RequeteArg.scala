import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source

object RequeteArg {


  def main(args: Array[String]): Unit = {


  val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "true").set("spark.sql.caseSensitive", "false")

  val spark = SparkSession.builder()
    .master("local")
    .appName("requeteapp")
    .getOrCreate()

  if (args.length < 1) {
    System.err.println(
      "Argument number's is not respected")
    System.exit(1)
  }

 val argFile =args(0)
 val argumentFile = Source.fromFile(argFile)
 val argLines = argumentFile.mkString.split("\n")

 val dataFile: String= argLines(0).trim
 val queryFile: String = argLines(1).trim
 val resultFile: String = argLines(2).trim

 QueryProcess.dataFileRead(dataFile)
 val queryResult= QueryProcess.queryRead (queryFile )
 QueryProcess.writeData (queryResult , resultFile)


    Thread.sleep(100000000)
  }
}

