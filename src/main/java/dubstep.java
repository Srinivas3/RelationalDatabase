import java.io.*;
import java.util.Scanner;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class dubstep {

    public static void main(String args[])throws ParseException, FileNotFoundException {

        System.out.println("entered line 14");
        Scanner scan = new Scanner(System.in);

        while (scan.hasNextLine()){
            String next = scan.nextLine();
            System.out.println(next);
            StringReader input = new StringReader(next);
            CCJSqlParser parser = new CCJSqlParser(input);
            Statement query = parser.Statement();
            System.out.println(query);
            if (query instanceof Select){
                System.out.println("entered line 25");
                SelectBody body = ((Select) query).getSelectBody();

                if (((PlainSelect) body).getFromItem() instanceof Table){
                    Table table = (Table ) ((PlainSelect) body).getFromItem();

                    String tableName = table.getName();
                    try{
                        File f = new File("data/" + tableName +".csv ");
//                        System.out.println(f.getAbsolutePath());
                        BufferedReader reader = new BufferedReader(new FileReader(f.getAbsoluteFile()));

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
