package utils;


import Indexes.PrimaryIndex;
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
    public static Map<String,Long> colToByteCnt = new HashMap<String,Long>();
    public static Map<String, Integer> tableToLines = new HashMap<String, Integer>();
    public static Map<String, PrimaryIndex> colToPrimIndex = new HashMap<String, PrimaryIndex>();
    public static Map<String, TreeMap<PrimitiveValue, List<Integer>>> colToSecIndex =
            new HashMap<String, TreeMap<PrimitiveValue, List<Integer>>>();
    public static Map<String,byte[]> cachedCols = new HashMap<String,byte[]>();
    public static Map<String, String> colToIndexType = new HashMap<String, String>();
    public static Map<String,ColumnDefinition> colToColDef = new HashMap<String,ColumnDefinition>();
    public static Map<String,Map<String,Integer>> viewToSchema = new HashMap<String,Map<String,Integer>>();
    public static Map<String,Expression> viewToExpression = new HashMap<String,Expression>();
    public static boolean isSameTable(String table, String col) {
        String[] partsCol = col.split("\\.");
        if (partsCol.length == 2) {
            return table.equalsIgnoreCase(partsCol[0]);
        }
        return false;
    }

    public static PrimitiveValue getPrimitiveValue(String dataType, String primValInString) {
        if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar")) {
            return new StringValue(primValInString);
        } else if (dataType.equalsIgnoreCase("date")) {
            return new DateValue(primValInString);
        } else if (dataType.equalsIgnoreCase("int")) {
            return new LongValue(primValInString);
        } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
            return new DoubleValue(primValInString);
        } else {
            return null;
        }
    }
    public static boolean isCachable(String tableColName){
        return false;
    }


    public static PrimitiveValue getPrimitiveValue(Expression expression){
        if(expression instanceof StringValue){
            return (StringValue)expression;
        }else if(expression instanceof DoubleValue){
            return (DoubleValue)expression;
        }else if(expression instanceof LongValue){
            return (LongValue) expression;
        }else if(expression instanceof DateValue){
            return (DateValue) expression;
        }
        else {
            return null;
        }
    }

    public static Map<Integer, String> getIdxToCol(Map<String, Integer> colToIdx) {
        Set<String> colSet = colToIdx.keySet();
        Map<Integer, String> idxToCol = new HashMap<Integer, String>();
        for (String col : colSet) {
            idxToCol.put(colToIdx.get(col), col);
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

    public static String getColName(String colKey) {
        String[] parts = colKey.split("\\.");
        if (parts.length == 1) {
            return parts[0];
        } else {
            return parts[1];
        }

    }

    public static Column getColumn(String colName) {
        Column column = new Column();
        String[] parts = colName.split("\\.");
        if (parts.length == 1) {
            column.setColumnName(parts[0]);
        } else {
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
                String parts[] = key.split("\\.");
                String keyCol = null;
                if (parts.length == 2) {
                    keyCol = parts[1];
                } else {
                    keyCol = parts[0];
                }

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
