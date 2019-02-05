import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.StringReader;
import java.util.Scanner;

public class dubstep {

    public static void main (String args[]) throws ParseException {


        new ProcessQuery().processQuery();

    }

}
