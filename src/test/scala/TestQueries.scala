import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level



object TestQueries {



  def main(args: Array[String]): Unit = {
    println("Hello, world!")

    var args_to_main: Array[String] = new Array[String](1)
    args_to_main(0) = "in-mem"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Database testing")
      .master("local[*]")
//      .config("master", "local[*]")
      .getOrCreate()
    import spark.implicits._

    val files = getListOfFiles("data/queries/")
    for (file <- files){
      dubstep.Main.main(args_to_main);

      import java.io.ByteArrayInputStream
      val in = new ByteArrayInputStream("My string".getBytes())
      System.setIn(in)
      System.setIn(new ByteArrayInputStream("My string".getBytes()))
      System.out.println("TEMP - 2")
      System.setIn(new ByteArrayInputStream("My string".getBytes()))
      System.out.println("TEMP - 2")

    }

  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

}
