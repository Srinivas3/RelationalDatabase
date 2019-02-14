package dubstep

import java.io.{BufferedReader, File, FileReader, StringReader}
import java.util.Scanner

import net.sf.jsqlparser.parser.CCJSqlParser
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectBody}

object Main {

  def main(args: Array[String]): Unit = {

    val scan = new Scanner(System.in)

    while ( {
      scan.hasNextLine
    }) {
      val input = new StringReader(scan.nextLine())
      val parser = new CCJSqlParser(input)
      val query = parser.Statement


      if (query.isInstanceOf[Select]){
        val body = query.asInstanceOf[Select].getSelectBody
        if (body.asInstanceOf[PlainSelect].getFromItem.isInstanceOf[Table]){

          val table = body.asInstanceOf[PlainSelect].getFromItem.asInstanceOf[Table]

          val tableName = table.getName
          try {
            val f = new File("data/" + tableName + ".csv")
            var reader = new BufferedReader(new FileReader(f))
            var line: String = null
            while ( {
              (line = reader.readLine()) != null
            }) System.out.println(line)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }

        }
      }

    }
  }

}
