package operators.joins;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
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

    private boolean isFirstCall;
    List<List<PrimitiveValue>> leftMatchedTuples;
    private JoinAlgorithm joinAlgorithm;
    private Map<String, PrimitiveValue> mergedTuple;
    private Map<String, Integer> schema;


    public JoinOperator(Operator leftChild, Operator rightChild, Join join) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.join = join;
        setSchema();
        isFirstCall = true;
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
        }
        if (join.isSimple()) {
            return simpleJoinNext();
        } else {
            return equiJoinNext();
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

    private Map<String, PrimitiveValue> equiJoinNext() {
        if (isFirstCall) {
            if (Utils.inMemoryMode) {
                joinAlgorithm = new OnePassHashJoin(this);
            } else {
                joinAlgorithm = new SortMergeJoin(this);
            }
            isFirstCall = false;
        }
        return joinAlgorithm.getNextValue();
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
