package operators.joins;

import net.sf.jsqlparser.expression.PrimitiveValue;
import operators.Operator;
import utils.Utils;

import java.util.*;
public class OnePassHashJoin implements  JoinAlgorithm{
    private JoinOperator joinOperator;
    private EquiJoinUtils equiJoinUtils;
    private boolean isFirstCall;
    private Operator leftChild;
    private Operator rightChild;
    private Map<String,PrimitiveValue> leftChildTuple;
    private Map<String,PrimitiveValue> rightChildTuple;
    private Map<String, List<List<PrimitiveValue>>> leftBuckets;
    private Iterator<List<PrimitiveValue>> leftBucketItr;
    private List<List<PrimitiveValue>> leftBucket;
    private String rightHash;

    public OnePassHashJoin(JoinOperator joinOperator){
        this.joinOperator = joinOperator;
        equiJoinUtils = new EquiJoinUtils(joinOperator);
        this.isFirstCall = true;
        this.leftChild = joinOperator.getLeftChild();
        this.rightChild = joinOperator.getRightChild();

        this.leftChildTuple = joinOperator.getLeftChildTuple();
        this.rightChildTuple = joinOperator.getRightChildTuple();
    }
    public Map<String, PrimitiveValue> getNextValue() {
        if (isFirstCall) {
            hashLeftTuples();
            advanceRight();
            isFirstCall = false;
        }
        if (leftBucketItr != null && leftBucketItr.hasNext()) {
            List<PrimitiveValue> matchedLeftTupleSerialized = leftBucketItr.next();
            Map<String, PrimitiveValue> matchedLeftTuple = Utils.convertToMap(matchedLeftTupleSerialized, joinOperator.getLeftIdxToColName());
            return joinOperator.merge(matchedLeftTuple, rightChildTuple);
        } else {
            advanceRight();
            if (rightChildTuple == null)
                return null;
            return getNextValue();
        }
    }

    private void hashLeftTuples() {
        leftBuckets = new HashMap<String, List<List<PrimitiveValue>>>();
        while (leftChildTuple != null) {
            String leftHash = getLeftHash();
            List<List<PrimitiveValue>> tempBucket = leftBuckets.get(leftHash);
            if (tempBucket != null)
                tempBucket.add(Utils.convertToList(leftChildTuple,leftChild.getSchema()));
            else {
                tempBucket = new ArrayList<List<PrimitiveValue>>();
                tempBucket.add(Utils.convertToList(leftChildTuple, leftChild.getSchema()));
                leftBuckets.put(leftHash, tempBucket);
            }
            leftChildTuple = leftChild.next();
        }
    }

    private String getLeftHash() {
        StringBuilder hashBuilder = new StringBuilder();
        String delimiter = "";
        for (List<String> colPair : equiJoinUtils.getJoinColPairs()) {
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
        for (List<String> colPair : equiJoinUtils.getJoinColPairs()) {
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


}
