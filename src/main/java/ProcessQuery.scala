import java.io.{File, StringReader}
import java.util.Scanner

import net.sf.jsqlparser.parser.CCJSqlParser
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.PlainSelect

class ProcessQuery {

  def main(args:Array[String]) ={
    
    val scan = new Scanner(System.in)

    while ( {
      scan.hasNext
    }) {
      val input = new StringReader(scan.next)
      val parser = new CCJSqlParser(input)
      val query = parser.Statement

      if (query.isInstanceOf[PlainSelect]) {

        if (query.asInstanceOf[PlainSelect].getFromItem().isInstanceOf[Table]){

          val table = query.asInstanceOf[PlainSelect].getFromItem().asInstanceOf[Table]


        }

        query match {
          case table : Table => {

            val tableName = table.getName()
            import java.io.{BufferedReader, FileReader, IOException}

            var reader :BufferedReader = null
            try {
              val file = new File("data/" + tableName)
              reader = new BufferedReader(new FileReader(file))
              var line: String = null
              while ( {
                (line = reader.readLine) != null
              }) System.out.println(line)
            } catch {
              case e: IOException =>
                e.printStackTrace()
            } finally try
              reader.close()
            catch {
              case e: IOException =>
                e.printStackTrace()
            }

          }
        }

      }


    }
  }

}
