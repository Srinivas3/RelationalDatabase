package schema;


import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

import java.util.HashMap;

public class Utils {
    public static Map<String, List<ColumnDefinition>> nameToColDefs = new HashMap<String, List<ColumnDefinition>>();
    public static boolean inMemoryMode = true;

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

    public static Map<String, PrimitiveValue> deserialize(List<PrimitiveValue> serializedTuple, Map<Integer, String> idxToColName) {
        if (serializedTuple == null)
            return null;
        int i = 0;
        Map<String, PrimitiveValue> deserializedTuple = new LinkedHashMap<String, PrimitiveValue>();
        for (PrimitiveValue colVal : serializedTuple) {
            deserializedTuple.put(idxToColName.get(i), colVal);
            i++;
        }
        return deserializedTuple;
    }

    public static List<PrimitiveValue> serialize(Map<String, PrimitiveValue> deserializedTuple, Map<String, Integer> colNameToIdx) {
        if (deserializedTuple == null)
            return null;
        List<PrimitiveValue> serializedTuple = new ArrayList<PrimitiveValue>();
        Set<String> colNames = colNameToIdx.keySet();
        for (String colName : colNames)
            serializedTuple.add(deserializedTuple.get(colName));
        return serializedTuple;

    }

    public static void fillColIdx(Map<String,PrimitiveValue> childTuple,Map<String,Integer> colNameToIdx,Map<Integer,String> idxToColName){
        if (childTuple == null)
            return;
        Set<String> colNames = childTuple.keySet();
        int i = 0;
        for(String colName: colNames){
            colNameToIdx.put(colName,i);
            idxToColName.put(i,colName);
            i++;
        }
    }



}
