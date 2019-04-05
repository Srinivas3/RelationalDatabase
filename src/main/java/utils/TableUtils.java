package utils;


import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

import java.util.HashMap;

public class TableUtils {

    public static Map<String, List<ColumnDefinition>> nameToColDefs = new HashMap<String, List<ColumnDefinition>>();

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
}
