package utils;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

import java.util.HashMap;

public class Utils {
    public static Map<String, List<ColumnDefinition>> nameToColDefs = new HashMap<String, List<ColumnDefinition>>();
    public static boolean inMemoryMode = true;
    public static int diskCacheCnt = 0;

    public static boolean isSameTable(String table, String col) {
        String[] partsCol = col.split("\\.");
        if (partsCol.length == 2) {
            return table.equalsIgnoreCase(partsCol[0]);
        }
        return false;
    }
    public static Map<Integer,String> getIdxToCol(Map<String,Integer> colToIdx ){
        Set<String> colSet = colToIdx.keySet();
        Map<Integer,String> idxToCol = new HashMap<Integer, String>();
        for(String col:colSet){
            idxToCol.put(colToIdx.get(col),col);
        }
        return idxToCol;
    }

    public static boolean areColsEqual(String col1, String col2) {
        String[] partsCol1 = col1.split("\\.");
        String[] partsCol2 = col2.split("\\.");
        if (partsCol1.length == 2) {
            col1 = partsCol1[1];
        }
        if (partsCol2.length == 2) {
            col2 = partsCol2[1];
        }
        return col1.equals(col2);

    }
    public static String getColName(String colKey){
        String[] parts = colKey.split("\\.");
        if (parts.length == 1){
            return parts[0];
        }
        else{
            return parts[1];
        }

    }
    public static Column getColumn(String colName){
        Column column = new Column();
        String[] parts = colName.split("\\.");
        if (parts.length == 1){
           column.setColumnName(parts[0]);
        }
        else{
            column.setTable(new Table(parts[0]));
            column.setColumnName(parts[1]);
        }
        return column;
    }

    public static PrimitiveValue getColValue(String tableName, String colName, Map<String, PrimitiveValue> tuple) {
        PrimitiveValue primVal = tuple.get(colName);
        if (primVal != null)
            return primVal;
        else if (tableName != null)
            return tuple.get(tableName + "." + colName);
        else {
            for (String key : tuple.keySet()) {
                String keyCol = key.split("\\.")[1];
                if (colName.equals(keyCol))
                    return tuple.get(key);
            }
        }
        return null;
    }

    public static Map<String, PrimitiveValue> convertToMap(List<PrimitiveValue> serializedTuple, Map<Integer, String> idxToColName) {
        if (serializedTuple == null)
            return null;
        int i = 0;
        //TODO don't create a new map every time this method is called, instead pass the map as an argument
        Map<String, PrimitiveValue> deserializedTuple = new LinkedHashMap<String, PrimitiveValue>();
        for (PrimitiveValue colVal : serializedTuple) {
            deserializedTuple.put(idxToColName.get(i), colVal);
            i++;
        }
        return deserializedTuple;
    }

    public static List<PrimitiveValue> convertToList(Map<String, PrimitiveValue> deserializedTuple,
                                                     Map<String, Integer> colNameToIdx) {
        if (deserializedTuple == null)
            return null;
        //TODO don't create a new map every time this method is called, instead pass the map as an argument
        List<PrimitiveValue> serializedTuple = new ArrayList<PrimitiveValue>();
        Set<String> colNames = colNameToIdx.keySet();
        for (String colName : colNames)
            serializedTuple.add(deserializedTuple.get(colName));
        return serializedTuple;

    }

    public static void fillColIdx(Map<String, PrimitiveValue> childTuple, Map<String, Integer> colNameToIdx, Map<Integer, String> idxToColName) {
        if (childTuple == null)
            return;
        Set<String> colNames = childTuple.keySet();
        int i = 0;
        for (String colName : colNames) {
            colNameToIdx.put(colName, i);
            idxToColName.put(i, colName);
            i++;
        }
    }


}
