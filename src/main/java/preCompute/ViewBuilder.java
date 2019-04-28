package preCompute;

import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Select;
import operators.Operator;
import utils.Constants;
import utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import  java.util.*;

public class ViewBuilder {

    private Join ordersLineItemJoin;
    private List<String> queries;
    private String queriesFile = "queriesFile";
    private int viewCnt = 0;
    private PreProcessor preProcessor;
    public ViewBuilder(){
        preProcessor = new PreProcessor();
    }
    public  void buildViews(){
        addQueries();
        writeQueriesToDisk();
        File viewQueriesFile = new File(queriesFile);
        try {
            FileInputStream fileInputStream = new FileInputStream(viewQueriesFile);
            runQueries(fileInputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void runQueries(InputStream inputStream) throws ParseException {
        CCJSqlParser ccjSqlParser = new CCJSqlParser(inputStream);
        Statement statement = null;
        int i = 1;
        while((statement = ccjSqlParser.Statement())!= null){
            Select select = (Select)statement;
            Operator operator = Main.handleSelect(select);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(System.out));
            try {
                writeViewToColumnStore(operator);
            } catch (Exception e) {
                e.printStackTrace();
            }
//            System.out.println(i + " view built");
            i++;
        }

    }

    private void writeViewToColumnStore(Operator operator) {
        String viewName = "view" + ++viewCnt;
        saveViewToSchema(viewName,operator);
        List<String> tableColNames = new ArrayList<String>();
        tableColNames.addAll(operator.getSchema().keySet());
        DataOutputStream[]  dataOutputStreams = preProcessor.openDataOutputStreams(tableColNames,viewName);
        Map<String,PrimitiveValue> tuple = null;
        while((tuple = operator.next())!= null){
            int i = 0;
            for (String tableColName: tableColNames){
                preProcessor.writeBytes(dataOutputStreams[i],tuple.get(tableColName));
                i++;
            }
        }
        preProcessor.flushAndClose(dataOutputStreams);
    }

    private void saveViewToSchema(String viewName, Operator operator) {
        Utils.viewToSchema.put(viewName,operator.getSchema());
        Set<String> schemaKeySet = operator.getSchema().keySet();
        File file = new File(Constants.VIEW_SCHEMA_DIR,viewName);
        try{
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
            for (String tableColName: schemaKeySet){
                bufferedWriter.write(tableColName);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            bufferedWriter.close();

        }
        catch(Exception e){
            e.printStackTrace();
        }


    }

    private void writeQueriesToDisk() {
        File viewQueriesFile = new File(queriesFile);
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(viewQueriesFile);
            BufferedWriter  bufferedWriter = new BufferedWriter(fileWriter);
            for (String query:queries){
                bufferedWriter.write(query);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void addQueries(){
        queries = new ArrayList<String>();
        queries.add("SELECT * FROM REGION,NATION WHERE NATION.REGIONKEY = REGION.REGIONKEY;");
        queries.add("SELECT * FROM REGION,NATION,CUSTOMER WHERE NATION.REGIONKEY = REGION.REGIONKEY AND CUSTOMER.NATIONKEY = NATION.NATIONKEY;");
        queries.add("SELECT * FROM REGION,NATION,CUSTOMER,ORDERS WHERE NATION.REGIONKEY = REGION.REGIONKEY AND CUSTOMER.NATIONKEY = NATION.NATIONKEY AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY;");
        queries.add("SELECT * FROM CUSTOMER,ORDERS WHERE CUSTOMER.CUSTKEY = ORDERS.CUSTKEY;");
        queries.add("SELECT * FROM NATION,CUSTOMER WHERE CUSTOMER.NATIONKEY = NATION.NATIONKEY;");

    }
}
