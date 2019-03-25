package operators;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import schema.TableUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.*;

public class TableScan implements Operator{
    static final String format=".dat";
    Table table;
    String filePath;
    BufferedReader br;
    List<ColumnDefinition> colDefs;
    Map<String, PrimitiveValue> tuple;
    String tableName;
    public TableScan(Table table){
        this.table = table;
        this.filePath = "data/"+ table.getName() + format;
        this.colDefs = TableUtils.nameToColDefs.get(table.getName());
        tuple = new LinkedHashMap<String, PrimitiveValue>();
        setTableName();
        init();
    }
    private void setTableName(){
        String alias = table.getAlias();
        if (alias != null)
            this.tableName = alias;
        else
            this.tableName = table.getName();

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
            String tableColName = this.tableName + "." + colName;
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
        if (dataType.equals("string") || dataType.equals("char") || dataType.equals("varchar"))
            return new StringValue(value);
        else if (dataType.equals("int"))
            return new LongValue(value);
        else if (dataType.equals("decimal") || dataType.equals("float"))
            return new DoubleValue(value);
        else if (dataType.equals("date"))
            return new DateValue(value);
        else
            return null;
    }
}
