package utils;


import Indexes.PrimaryIndex;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

import java.util.HashMap;

public class Utils {
    public static Map<String, List<ColumnDefinition>> nameToColDefs = new HashMap<String, List<ColumnDefinition>>();
    public static boolean inMemoryMode = true;
    public static int diskCacheCnt = 0;
    public static Map<String, Long> colToByteCnt = new HashMap<String, Long>();
    public static Map<String, Integer> tableToLines = new HashMap<String, Integer>();
    public static Map<String, PrimaryIndex> colToPrimIndex = new HashMap<String, PrimaryIndex>();
    public static Map<String, TreeMap<PrimitiveValue, List<Integer>>> colToSecIndex =
            new HashMap<String, TreeMap<PrimitiveValue, List<Integer>>>();
    public static Map<String, byte[]> cachedCols = new HashMap<String, byte[]>();
    public static Map<String, String> colToIndexType = new HashMap<String, String>();
    public static Map<String, ColumnDefinition> colToColDef = new HashMap<String, ColumnDefinition>();
    public static Map<String, Map<String, Integer>> viewToSchema = new HashMap<String, Map<String, Integer>>();
    public static Map<String, Expression> viewToExpression = new HashMap<String, Expression>();
    public static Set<String> rangeScannedCols = new HashSet<String>();

    public static boolean isSameTable(String table, String col) {
        String[] partsCol = col.split("\\.");
        if (partsCol.length == 2) {
            return table.equalsIgnoreCase(partsCol[0]);
        }
        return false;
    }

    public static void populateRangeScanData() {
        rangeScannedCols.add("LINEITEM.SHIPDATE");
        rangeScannedCols.add("ORDERS.ORDERDATE");
    }

    public static List<Expression> getRangeExpressions(BinaryExpression basicExpression) {
        GreaterThanEquals greaterThanEqualsExp = getGreaterEqualsExp(basicExpression);
        MinorThan minorThanExp = getMinorThanExp(basicExpression);
        Column column = getColumn(basicExpression);
        String currMaxDate;
        String currMinDate;
        if (greaterThanEqualsExp == null) {
            currMinDate = Constants.MIN_DATE_STR;
        } else {
            DateValue datePrimitiveVal = getDateValueFromFunction((Function) greaterThanEqualsExp.getRightExpression());
            currMinDate = getCurrNewYear(datePrimitiveVal);
            if (currMinDate.compareTo(Constants.MIN_DATE_STR) < 0){
                currMinDate  = Constants.MIN_DATE_STR;
            }
        }
        if (minorThanExp == null) {
            currMaxDate = Constants.MAX_DATE_STR;
        } else {
            DateValue datePrimitiveVal = getDateValueFromFunction((Function) minorThanExp.getRightExpression());
            currMaxDate = getNearestNextNewYear(datePrimitiveVal);
            if (currMaxDate.compareTo(Constants.MAX_DATE_STR) > 0){
                currMaxDate = Constants.MAX_DATE_STR;
            }
        }
        List<Expression> rangeExps = new ArrayList<Expression>();
        String runningDate = currMinDate;
        DateValue runningDateValue = new DateValue(runningDate);
        while (runningDate.compareTo(currMaxDate) < 0) {
            GreaterThanEquals currGreaterThanEquals = new GreaterThanEquals(column, getDateFunction(runningDateValue));
            runningDate = getNextNewYear(runningDateValue);
            runningDateValue = new DateValue(runningDate);
            MinorThan currMinorThan = new MinorThan(column, getDateFunction(runningDateValue));
            rangeExps.add(new AndExpression(currGreaterThanEquals, currMinorThan));
        }
        return rangeExps;
    }

    private static Column getColumn(BinaryExpression basicExpression) {
        if (basicExpression instanceof AndExpression){
            return getColumn((BinaryExpression) basicExpression.getLeftExpression());
        }
        if (basicExpression.getLeftExpression() instanceof Column){
            return (Column)basicExpression.getLeftExpression();
        }
        if (basicExpression.getRightExpression() instanceof  Column){
            return (Column)basicExpression.getRightExpression();
        }

        return null;
    }
    private static String getCurrNewYear(DateValue dateValue){
        String[] parts = dateValue.toRawString().split("-");
        String newYear = String.valueOf(Integer.valueOf(parts[0]));
        return newYear + "-01-01";

    }
    private static String getNextNewYear(DateValue runningDateValue) {
        String[] parts = runningDateValue.toRawString().split("-");
        String newYear = String.valueOf(Integer.valueOf(parts[0]) + 1);
        return newYear + "-01-01";
    }

    private static MinorThan getMinorThanExp(BinaryExpression basicExpression) {
        if (basicExpression instanceof AndExpression) {
            MinorThan minorThanExp = getMinorThanExp((BinaryExpression) basicExpression.getLeftExpression());
            if (minorThanExp == null) {
                return getMinorThanExp((BinaryExpression) basicExpression.getRightExpression());
            } else {
                return minorThanExp;
            }
        } else {
            if (basicExpression instanceof MinorThan) {
                return (MinorThan) basicExpression;
            } else if (basicExpression instanceof MinorThanEquals) {
                Column column = (Column) basicExpression.getLeftExpression();
                DateValue dateValue = getDateValueFromFunction((Function) basicExpression.getRightExpression());
                DateValue nearestNextDateValue = new DateValue(getNextNewYear(dateValue));
                Function dateFunction = getDateFunction(nearestNextDateValue);
                return new MinorThan(column, dateFunction);
            } else {
                return null;
            }

        }
    }

    private static GreaterThanEquals getGreaterEqualsExp(BinaryExpression basicExpression) {
        if (basicExpression instanceof AndExpression) {
            GreaterThanEquals greaterThanEqualsExp = getGreaterEqualsExp((BinaryExpression) basicExpression.getLeftExpression());
            if (greaterThanEqualsExp == null) {
                return getGreaterEqualsExp((BinaryExpression) basicExpression.getRightExpression());
            } else {
                return greaterThanEqualsExp;
            }
        } else {
            if (basicExpression instanceof GreaterThanEquals) {
                return (GreaterThanEquals) basicExpression;
            } else if (basicExpression instanceof GreaterThan) {
                return new GreaterThanEquals(basicExpression.getLeftExpression(), basicExpression.getRightExpression());
            } else {
                return null;
            }
        }

    }

    private static DateValue getDateValueFromFunction(Function function) {
        StringValue stringValue = (StringValue) function.getParameters().getExpressions().get(0);
        return new DateValue(stringValue.toRawString());
    }

    public static Function getDateFunction(DateValue dateValue) {
        Function function = new Function();
        function.setName("DATE");
        List<Expression> expressions = new ArrayList<Expression>();
        expressions.add(new StringValue(dateValue.toRawString()));
        ExpressionList expressionList = new ExpressionList();
        expressionList.setExpressions(expressions);
        function.setParameters(expressionList);
        return function;
    }

    private static String getNearestNextNewYear(DateValue datePrimitiveVal) {
        if (checkIfNewYear(datePrimitiveVal)) {
            return datePrimitiveVal.toRawString();
        } else {
            String currYear = datePrimitiveVal.toRawString().split("-")[0];
            String nextYear = String.valueOf(Integer.valueOf(currYear) + 1);
            return nextYear + "-01-01";
        }
    }

    private static String getNearestPrevNewYear(DateValue datePrimitiveVal) {
        if (checkIfNewYear(datePrimitiveVal)) {
            return datePrimitiveVal.toRawString();
        } else {
            String currYear = datePrimitiveVal.toRawString().split("-")[0];
            String nextYear = String.valueOf(Integer.valueOf(currYear) - 1);
            return nextYear + "-01-01";
        }
    }

    private static boolean checkIfNewYear(DateValue datePrimitiveVal) {
        String dateStr = datePrimitiveVal.toRawString();
        String[] parts = dateStr.split("-");
        int month = Integer.valueOf(parts[1]);
        int day = Integer.valueOf(parts[2]);
        return month == 1 && day == 1;
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

    public static boolean isCachable(String tableColName) {
        return false;
    }


    public static PrimitiveValue getPrimitiveValue(Expression expression) {
        if (expression instanceof StringValue) {
            return (StringValue) expression;
        } else if (expression instanceof DoubleValue) {
            return (DoubleValue) expression;
        } else if (expression instanceof LongValue) {
            return (LongValue) expression;
        } else if (expression instanceof DateValue) {
            return (DateValue) expression;
        } else {
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
