import java.io.*;
import java.util.Scanner;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class dubstep {

    public static void main(String args[])throws ParseException, FileNotFoundException {

        Scanner scan = new Scanner(System.in);

        while (scan.hasNext()){
            StringReader input = new StringReader(scan.next());
            CCJSqlParser parser = new CCJSqlParser(input);
            Statement query = parser.Statement();

            if (query instanceof PlainSelect){

                if (((PlainSelect) query).getFromItem() instanceof Table){
                    Table table = (Table ) ((PlainSelect) query).getFromItem();

                    String tableName = table.getName();
                    try{
                        BufferedReader reader = new BufferedReader(new FileReader(new File("data/" + tableName)));
                        String line = null;
                        while ((line = reader.readLine()) != null){
                            System.out.println(line);
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }



                }

            }
        }
    }

}
