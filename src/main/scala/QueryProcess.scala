import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source


object QueryProcess {

  val spark = SparkSession.builder()
    .master("local")
    .appName("requeteapp")
    .getOrCreate()


  def  dataFileRead (dataFile : String): Unit =
  {
    spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(dataFile)
      .createGlobalTempView("Historical")
  }


  def  queryRead (queryFile:String): DataFrame =
  {
    val requetecontenu = Source.fromFile(queryFile).getLines()
    val Originalreq=  requetecontenu.next()   // next() => extraire la premiere string du iterator of string (ya9ra la valeur mbaed ya9ra li baadha ama houni j'ai qu'une seule donc yakraha heya)
    //println(Originalreq)                          // alt + =   => taatik type de retour de la fonction ou de la variable
    spark.sql(Originalreq)
  }



  def writeData (queryResult: DataFrame , resultFile:String) :Unit ={
    queryResult.write.format("csv")
      .option("header", "true")
      .save(resultFile)

    Thread.sleep(100000000)
  }
}
