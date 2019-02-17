package Operators;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import schema.TableUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.*;

public class TableScan implements Operator{
    Table table;
    String filePath;
    BufferedReader br;
    List<ColumnDefinition> colDefs;
    Map<String, PrimitiveValue> tuple;
    public TableScan(Table table){
        this.table = table;
        this.filePath = "data/"+ table.getName().toLowerCase() + ".dat";
        this.colDefs = TableUtils.nameToColDefs.get(table.getName());
        tuple = new LinkedHashMap<String, PrimitiveValue>();
        init();
    }

    public Map<String,PrimitiveValue> next(){
        String line = null;
        try{
            line = br.readLine();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        if (line == null){
            return null;
        }

        String[] colValues = line.split("\\|");
        int i = 0;
        for (ColumnDefinition colDef: colDefs) {
            String colName = colDef.getColumnName();
            String colVal = colValues[i];
            String dataType = colDef.getColDataType().getDataType().toLowerCase();
            PrimitiveValue primVal = getPrimitiveValue(dataType,colVal);
            String tableColName = table.getName() + "." + colName;
            tuple.put(tableColName,primVal);
            i++;
        }
        return tuple;
    }
    public void init(){
        try {
            br = new BufferedReader(new FileReader(new File(filePath)));
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
    private PrimitiveValue getPrimitiveValue(String dataType, String value){
        if (dataType.equals("string")){
            return new StringValue(value);
        }
        else if (dataType.equals("int")){
            return new LongValue(value);
        }
        else{
            return null;
        }

    }
}
