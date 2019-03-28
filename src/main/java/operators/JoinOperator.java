package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import schema.Utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

public class JoinOperator extends Eval implements Operator {
    Operator leftChild;
    Operator rightChild;
    Join join;
    Map<String, PrimitiveValue> leftChildTuple;
    Map<String, PrimitiveValue> rightChildTuple;
    Map<String, PrimitiveValue> currTuple;
    Map<String, Integer> colNameToIdx;
    Map<Integer, String> idxToColName;
    List<List<String>> joinColPairs;
    Map<String, List<List<PrimitiveValue>>> leftBuckets;
    Iterator<List<PrimitiveValue>> leftBucketItr;
    boolean isFirstCall;
    List<List<PrimitiveValue>> leftMatchedTuples;
    String rightHash;
    List<List<PrimitiveValue>> leftBucket;

    public JoinOperator(Operator leftChild, Operator rightChild, Join join) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.join = join;
        leftChildTuple = leftChild.next();
        rightChildTuple = rightChild.next();
        isFirstCall = true;
    }

    public PrimitiveValue eval(Column x) {
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, this.currTuple);
    }

    public Map<String, PrimitiveValue> next() {
        if (join.isSimple())
            return simpleJoinNext();
        else {
            if (isFirstCall)
                saveSchema();
            if (join.isNatural())
                return naturalJoinNext();
            else
                return equiJoinNext();
        }
    }

    private void saveSchema(){
        colNameToIdx = new LinkedHashMap<String,Integer>();
        idxToColName = new LinkedHashMap<Integer, String>();
        Utils.fillColIdx(leftChildTuple, colNameToIdx, idxToColName);
    }

    private Map<String, PrimitiveValue> equiJoinNext() {
        if (isFirstCall) {
            joinColPairs = new ArrayList<List<String>>();
            rightChildTuple = rightChild.next();
            Expression onExpression = join.getOnExpression();
            parseExpressionAndUpdateColPairs(onExpression);
        }
        if (Utils.inMemoryMode)
            return onePassHashJoinNext();
        else
            return sortMergeJoinNext();
    }

    private void parseExpressionAndUpdateColPairs(Expression onExpression) {
        if (onExpression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) onExpression;
            parseExpressionAndUpdateColPairs(andExpression.getLeftExpression());
            parseExpressionAndUpdateColPairs(andExpression.getRightExpression());
        } else {
            EqualsTo equalsToExpression = (EqualsTo) onExpression;
            Column firstCol = (Column) equalsToExpression.getLeftExpression();
            Column secondCol = (Column) equalsToExpression.getRightExpression();
            updateColPairs(firstCol.toString(), secondCol.toString());
        }
    }

    private void updateColPairs(String firstColName, String secondColName) {
        List<String> colPair = new ArrayList<String>();
        if (leftChildTuple.containsKey(firstColName) && rightChildTuple.containsKey(secondColName)) {
            colPair.add(firstColName);
            colPair.add(secondColName);
        } else {
            colPair.add(secondColName);
            colPair.add(firstColName);
        }
        joinColPairs.add(colPair);
    }

    private Map<String, PrimitiveValue> naturalJoinNext() {
        if (isFirstCall) {
            joinColPairs = new ArrayList<List<String>>();
            updateCommonCols();
        }
        if (Utils.inMemoryMode)
            return onePassHashJoinNext();
        else
            return sortMergeJoinNext();


    }

    private Map<String, PrimitiveValue> sortMergeJoinNext() {
        List<OrderByElement> leftOrderByElements = createOrderByElements(0);
        List<OrderByElement> rightOrderByElements = createOrderByElements(1);

        OrderByOperator orderedLeftChild = new OrderByOperator(leftOrderByElements, leftChild, leftChildTuple);
        OrderByOperator orderedRightChild = new OrderByOperator(rightOrderByElements, rightChild, rightChildTuple);

        //Utils.convertToList()
        //orderedLeftChild.compareTuples.compareMaps();

        return null;
    }



    private List<OrderByElement> createOrderByElements(int index) {
        List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
        for (List<String> joinColPair : joinColPairs){
            OrderByElement orderByElement = new OrderByElement();
            String leftCol = joinColPair.get(index);
            orderByElement.setExpression(getColumn(leftCol));
            orderByElements.add(orderByElement);
        }

        return orderByElements;
    }

    private Column getColumn(String column) {
        String[] tableAndCol = column.split("//.");
        Column col =  null;
        if (tableAndCol.length > 1){
            Table table = new Table();
            table.setName(tableAndCol[0]);
            String colName = tableAndCol[1];
            col= new Column(table, colName);

        } else {
            String colName = tableAndCol[1];
            col = new Column();
            col.setColumnName(colName);
        }
        return col;
    }

    private Map<String, PrimitiveValue> onePassHashJoinNext() {

        if (isFirstCall) {
            hashLeftTuples();
            advanceRight();
            isFirstCall = false;
        }
        if (leftBucketItr != null && leftBucketItr.hasNext()) {
            List<PrimitiveValue> matchedLeftTupleSerialized = leftBucketItr.next();
            Map<String, PrimitiveValue> matchedLeftTuple = Utils.convertToMap(matchedLeftTupleSerialized, idxToColName);
            return merge(matchedLeftTuple, rightChildTuple);
        } else {
            advanceRight();
            if (rightChildTuple == null)
                return null;
            return onePassHashJoinNext();
        }
    }


    private void hashLeftTuples() {
        leftBuckets = new HashMap<String, List<List<PrimitiveValue>>>();
        while (leftChildTuple != null) {
            String leftHash = getLeftHash();
            List<List<PrimitiveValue>> tempBucket = leftBuckets.get(leftHash);
            if (tempBucket != null)
                tempBucket.add(Utils.convertToList(leftChildTuple, colNameToIdx));
            else {
                tempBucket = new ArrayList<List<PrimitiveValue>>();
                tempBucket.add(Utils.convertToList(leftChildTuple, colNameToIdx));
                leftBuckets.put(leftHash, tempBucket);
            }
            leftChildTuple = leftChild.next();
        }
    }

    private String getLeftHash() {
        StringBuilder hashBuilder = new StringBuilder();
        String delimiter = "";
        for (List<String> colPair : joinColPairs) {
            String colName = colPair.get(0);
            String colVal = leftChildTuple.get(colName).toRawString();
            hashBuilder.append(delimiter);
            delimiter = "_";
            hashBuilder.append(colVal);
        }
        return hashBuilder.toString();
    }

    private String getRightHash() {
        StringBuilder hashBuilder = new StringBuilder();
        String delimiter = "";
        for (List<String> colPair : joinColPairs) {
            String colName = colPair.get(1);
            String colVal = rightChildTuple.get(colName).toRawString();
            hashBuilder.append(delimiter);
            delimiter = "_";
            hashBuilder.append(colVal);
        }
        return hashBuilder.toString();
    }


    private void advanceRight() {
        if (!isFirstCall)
            rightChildTuple = rightChild.next();
        if (rightChildTuple == null)
            return;
        rightHash = getRightHash();
        leftBucket = leftBuckets.get(rightHash);
        if (leftBucket == null)
            leftBucketItr = null;
        else
            leftBucketItr = leftBucket.iterator();
    }


    private void updateCommonCols() {
        Set<String> leftCols = leftChildTuple.keySet();
        Set<String> rightCols = rightChildTuple.keySet();
        for (String leftCol : leftCols) {
            if (rightCols.contains(leftCol)) {
                List<String> commonColPair = new ArrayList<String>();
                commonColPair.add(leftCol);
                commonColPair.add(leftCol);
                joinColPairs.add(commonColPair);
            }
        }

    }

    public void init() {

    }

    private Map<String, PrimitiveValue> simpleJoinNext() {
        if (leftChildTuple == null)
            return null;
        rightChildTuple = rightChild.next();
        if (rightChildTuple == null) {
            leftChildTuple = leftChild.next();
            rightChild.init();
            return next();
        }
        Map<String, PrimitiveValue> joinTuple = merge(leftChildTuple, rightChildTuple);
        this.currTuple = joinTuple;
        if (isValid(joinTuple))
            return joinTuple;
        else
            return next();

    }

    private Map<String, PrimitiveValue> merge(Map<String, PrimitiveValue> leftTuple, Map<String, PrimitiveValue> rightTuple) {
        Map<String, PrimitiveValue> mergedTuple = new LinkedHashMap<String, PrimitiveValue>();
        for (String key : leftTuple.keySet())
            mergedTuple.put(key, leftTuple.get(key));
        for (String key : rightTuple.keySet())
            mergedTuple.put(key, rightTuple.get(key));
        return mergedTuple;
    }

    private boolean isValid(Map<String, PrimitiveValue> joinTuple) {
        if (join.isSimple()) {
            return true;
        }
        return false;
    }


}
