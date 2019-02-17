package dubstep;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

import Operators.Operator;
import buildtree.TreeBuilder;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import schema.TableUtils;

public class Main {

    public static void main(String args[])throws ParseException, FileNotFoundException {
        System.out.print("$>");
        CCJSqlParser parser = new CCJSqlParser(System.in);
        Statement statement;
        while ((statement = parser.Statement())!= null){
            if (statement instanceof Select){
                Operator root = handleSelect((Select)statement);
                displayOuput(root);
            }
            else if (statement instanceof CreateTable){
                CreateTable createTable = (CreateTable)statement;
                String tableName = createTable.getTable().getName();
                List<ColumnDefinition> colDefs = createTable.getColumnDefinitions();
                TableUtils.nameToColDefs.put(tableName,colDefs);
            }
            else{
                System.out.println("Invalid Query");
            }
            System.out.print("$>");
        }
    }
    public static Operator handleSelect(Select select){
        TreeBuilder treeBuilder = new TreeBuilder();
        SelectBody selectBody = select.getSelectBody();
        return treeBuilder.handleSelectBody(selectBody);
    }
    public static void displayOuput(Operator operator){
        Map<String, PrimitiveValue> tuple;
        while((tuple = operator.next())!= null ){
            StringBuilder sb = new StringBuilder();
            Set<String> keySet = tuple.keySet();
            int i = 0;
             for(String key: keySet){
                sb.append(tuple.get(key));
                if (i < keySet.size()-1)
                    sb.append("|");
                i += 1;
            }
            System.out.println(sb.toString());
        }
    }

}
