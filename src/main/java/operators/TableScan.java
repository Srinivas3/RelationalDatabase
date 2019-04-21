package operators;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Utils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.*;

import dubstep.Main;


public class TableScan implements Operator {
    static final String format = ".csv";
    Table table;
    String filePath;
    DataInputStream dataInputStream;
    List<ColumnDefinition> colDefs;
    Map<String, PrimitiveValue> tuple;
    String tableName;
    Map<String, Integer> schema;
    int linesScanned;
    int totalLines;

    public TableScan(Table table) {
        this.table = table;
        this.filePath = "data/" + table.getName() + format;
        this.colDefs = Utils.nameToColDefs.get(table.getName());
        tuple = new LinkedHashMap<String, PrimitiveValue>();
        linesScanned = 0;
        totalLines = Utils.tableToLines.get(table.getName());
        setTableName();
        setSchema();
        init();
    }

    private void setSchema() {
        schema = new LinkedHashMap<String, Integer>();
        int colCounter = 0;
        for (ColumnDefinition colDef : colDefs) {
            String colName = colDef.getColumnName();
            String tableColName = this.tableName + "." + colName;
            schema.put(tableColName, colCounter);
            colCounter++;
        }
    }

    public Map<String, Integer> getSchema() {
        return schema;
    }

    private void setTableName() {
        String alias = table.getAlias();
        if (alias != null)
            this.tableName = alias;
        else
            this.tableName = table.getName();

    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, PrimitiveValue> next() {
        try {
            if (linesScanned < totalLines) {
                readTuple();
                linesScanned++;
                return tuple;
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void readTuple() throws IOException {
        for (ColumnDefinition colDef : colDefs) {
            String colName = colDef.getColumnName();
            String dataType = colDef.getColDataType().getDataType().toLowerCase();
            PrimitiveValue primVal = getPrimitiveValue(dataType);
            String tableColName = this.tableName + "." + colName;
            tuple.put(tableColName, primVal);
        }
    }

    public void init() {
        try {
            if (dataInputStream != null) {
                dataInputStream.close();
            }
            File compressedTableFile = new File(Main.compressedTablesDir, tableName);
            FileInputStream fileInputStream = new FileInputStream(compressedTableFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            dataInputStream = new DataInputStream(bufferedInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private PrimitiveValue getPrimitiveValue(String dataType) throws IOException {
        if (dataType.equals("string") || dataType.equals("char") || dataType.equals("varchar") || dataType.equals("date")) {
            int numBytes = dataInputStream.readInt();
            byte byteArr[] = new byte[numBytes];
            dataInputStream.readFully(byteArr);
            String value = new String(byteArr);
            if (dataType.equals("date")) {
                return new DateValue(value);
            } else {
                return new StringValue(value);
            }
        } else if (dataType.equals("int")) {
            return new LongValue(dataInputStream.readInt());
        } else if (dataType.equals("decimal") || dataType.equals("float")) {
            return new DoubleValue(dataInputStream.readDouble());
        } else {
            return null;
        }
    }
}
