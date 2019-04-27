package operators.joins;

import Indexes.PrimaryIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import operators.*;
import utils.Utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

public class JoinOperator implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private Join join;
    private Map<String, PrimitiveValue> leftChildTuple;
    private Map<String, PrimitiveValue> rightChildTuple;
    private Map<String, Integer> leftColNameToIdx;
    private Map<String, Integer> rightColNameToIdx;
    private Map<Integer, String> leftIdxToColName;
    private Map<Integer, String> rightIdxToColName;
    private List<List<String>> joinColPairs;
    private boolean isFirstCall;
    List<List<PrimitiveValue>> leftMatchedTuples;
    private JoinAlgorithm joinAlgorithm;
    private Map<String, PrimitiveValue> mergedTuple;
    private Map<String, Integer> schema;

    public Map<String, Integer> getLeftColNameToIdx() {
        return leftColNameToIdx;
    }

    public void setLeftColNameToIdx(Map<String, Integer> leftColNameToIdx) {
        this.leftColNameToIdx = leftColNameToIdx;
    }

    public Map<String, Integer> getRightColNameToIdx() {
        return rightColNameToIdx;
    }

    public void setRightColNameToIdx(Map<String, Integer> rightColNameToIdx) {
        this.rightColNameToIdx = rightColNameToIdx;
    }

    public Map<Integer, String> getLeftIdxToColName() {
        return leftIdxToColName;
    }

    public void setLeftIdxToColName(Map<Integer, String> leftIdxToColName) {
        this.leftIdxToColName = leftIdxToColName;
    }

    public Map<Integer, String> getRightIdxToColName() {
        return rightIdxToColName;
    }

    public void setRightIdxToColName(Map<Integer, String> rightIdxToColName) {
        this.rightIdxToColName = rightIdxToColName;
    }


    public boolean isFirstCall() {
        return isFirstCall;
    }

    public void setFirstCall(boolean firstCall) {
        isFirstCall = firstCall;
    }


    public JoinOperator(Operator leftChild, Operator rightChild, Join join) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.join = join;
        setSchema();
        isFirstCall = true;
        if (!join.isSimple() && Utils.inMemoryMode) {
            setEquiJoinAlgorithm();
        }
        mergedTuple = new LinkedHashMap<String, PrimitiveValue>();
    }

    public Operator getLeftChild() {
        return leftChild;
    }

    public Operator getRightChild() {
        return rightChild;
    }

    public Map<String, PrimitiveValue> getLeftChildTuple() {
        return leftChildTuple;
    }

    public void setLeftChildTuple(Map<String, PrimitiveValue> leftChildTuple) {
        this.leftChildTuple = leftChildTuple;
    }

    public Map<String, PrimitiveValue> getRightChildTuple() {
        return rightChildTuple;
    }

    public void setRightChildTuple(Map<String, PrimitiveValue> rightChildTuple) {
        this.rightChildTuple = rightChildTuple;
    }

    public void setChild(String leftOrRight, Operator child) {
        if (leftOrRight.equals("left")) {
            this.leftChild = child;
        } else if (leftOrRight.equals("right")) {
            this.rightChild = child;
        } else {
            System.out.println("Invalid Child");
        }
    }

    private void setSchema() {
        schema = new LinkedHashMap<String, Integer>();
        Map<String, Integer> leftChildSchema = this.leftChild.getSchema();
        Map<String, Integer> rightChildSchema = this.rightChild.getSchema();

        Set<String> leftColNames = leftChildSchema.keySet();
        Set<String> rightColNames = rightChildSchema.keySet();

        int colCounter = 0;

        for (String leftCol : leftColNames) {
            schema.put(leftCol, colCounter);
            colCounter++;
        }

        for (String rightCol : rightColNames) {
            if (schema.get(rightCol) == null) {
                schema.put(rightCol, colCounter);
                colCounter++;
            }
        }


    }


    public Map<String, Integer> getSchema() {
        return schema;
    }

    public Map<String, PrimitiveValue> next() {
        if (joinAlgorithm instanceof IndexNestedLoopJoin) {
            return joinAlgorithm.getNextValue();
        }
        if (isFirstCall) {
            leftChildTuple = this.leftChild.next();
            rightChildTuple = this.rightChild.next();
            if (leftChildTuple == null || rightChildTuple == null) {
                return null;
            }
            leftColNameToIdx = leftChild.getSchema();
            leftIdxToColName = Utils.getIdxToCol(leftColNameToIdx);
            rightColNameToIdx = rightChild.getSchema();
            rightIdxToColName = Utils.getIdxToCol(rightColNameToIdx);
            if (!Utils.inMemoryMode) {
                joinAlgorithm = new SortMergeJoin(this);
            }
        }
        if (join.isSimple()) {
            return simpleJoinNext();
        } else {
            isFirstCall = false;
            return joinAlgorithm.getNextValue();
        }

    }

    private Map<String, PrimitiveValue> simpleJoinNext() {
        if (leftChildTuple == null || rightChildTuple == null) {
            return null;
        }
        if (isFirstCall) {
            joinAlgorithm = new BlockNestedLoopJoin(this);
            isFirstCall = false;
        }
        return joinAlgorithm.getNextValue();

    }

    public Join getJoin() {
        return join;
    }

    private void setEquiJoinAlgorithm() {
        Expression onExpression = join.getOnExpression();
        if (onExpression instanceof EqualsTo) {
            EqualsTo equalsToExp = (EqualsTo) onExpression;
            IndexNestedLoopJoin indexNestedLoopJoin;
            IndexScanOperator indexScan = getIndexScan(leftChild,equalsToExp);
            if (indexScan != null){
                String otherCol = getColBelongingToSchema(equalsToExp,rightChild.getSchema());
                joinAlgorithm =  new IndexNestedLoopJoin(this, indexScan, rightChild,otherCol);
                return;
            }
            indexScan = getIndexScan(rightChild,equalsToExp);
            if (indexScan != null){
                String otherCol = getColBelongingToSchema(equalsToExp,leftChild.getSchema());
                joinAlgorithm = new IndexNestedLoopJoin(this,indexScan,leftChild,otherCol);
                return;
            }
        }
        joinAlgorithm = new OnePassHashJoin(this);

    }

    private IndexScanOperator getIndexScan(Operator child, EqualsTo equalsToExp) {
        Expression filterCond = null;
        TableScan tableScan = null;
        if (child instanceof TableScan){
                 tableScan = (TableScan)child;
        }
        if (child instanceof SelectionOperator){
            SelectionOperator selectionOperator = (SelectionOperator)child;
            filterCond = selectionOperator.getWhereExp();
            Operator selectionChild = selectionOperator.getChild();
            if (selectionChild instanceof TableScan){
                tableScan = (TableScan)selectionChild;
            }
        }
        if (tableScan == null){
            return null;
        }
        String tableColName = getColBelongingToSchema(equalsToExp,tableScan.getSchema());
        PrimaryIndex primaryIndex = Utils.colToPrimIndex.get(tableColName);
        if (primaryIndex == null){
            return null;
        }
        return new IndexScanOperator("EqualsTo",filterCond,primaryIndex);
    }




    private String getColBelongingToSchema(EqualsTo equalsToExp, Map<String, Integer> schema) {
        Column leftCol = (Column) equalsToExp.getLeftExpression();
        Column rightCol = (Column) equalsToExp.getRightExpression();
        String leftColStr = leftCol.toString();
        String rightColStr = rightCol.toString();
        if (schema.containsKey(leftColStr)) {
            return leftColStr;
        } else {
            return rightColStr;
        }

    }

    public void init() {
        leftChild.init();
        rightChild.init();
    }


    public Map<String, PrimitiveValue> merge(Map<String, PrimitiveValue> leftTuple, Map<String, PrimitiveValue> rightTuple) {
        mergedTuple.clear();
        for (String key : leftTuple.keySet())
            mergedTuple.put(key, leftTuple.get(key));
        for (String key : rightTuple.keySet())
            mergedTuple.put(key, rightTuple.get(key));
        return mergedTuple;
    }


}
