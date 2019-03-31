package operators;

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
    Map<String, Integer> leftColNameToIdx;
    Map<String, Integer> rightColNameToIdx;
    Map<Integer, String> leftIdxToColName;
    Map<Integer, String> rightIdxToColName;
    List<List<String>> joinColPairs;
    Map<String, List<List<PrimitiveValue>>> leftBuckets;
    Iterator<List<PrimitiveValue>> leftBucketItr;
    boolean isFirstCall;
    List<List<PrimitiveValue>> leftMatchedTuples;
    String rightHash;
    List<List<PrimitiveValue>> leftBucket;
    List<List<PrimitiveValue>> leftBlock;
    List<List<PrimitiveValue>> rightBlock;
    Map<String, PrimitiveValue> previousLeftTuple = null;
    Map<String, PrimitiveValue> previousRightTuple = null;
    Map<String, PrimitiveValue> currentLeftTuple = null;
    Map<String, PrimitiveValue> currentRightTuple = null;
    List<List<PrimitiveValue>> leftElementsList = new ArrayList<List<PrimitiveValue>>();
    List<List<PrimitiveValue>> rightElementsList = new ArrayList<List<PrimitiveValue>>();
    Iterator<List<PrimitiveValue>> leftElementsListIterator = null;
    Iterator<List<PrimitiveValue>> rightElementsListIterator = null;

    List<OrderByElement> leftOrderByElements = null;
    List<OrderByElement> rightOrderByElements = null;
    OrderByOperator orderedLeftChild = null;
    OrderByOperator orderedRightChild = null;
    boolean lastMovedOnRight = true;
    boolean iterateOverChildren = true;
    private boolean listsNotExhausted = false;
    private boolean endOfChildrenReached = false;
    List<PrimitiveValue> currentRightElementInList = null;
    int blockSize;
    private boolean isLeftBlockExhausted;
    private boolean isRightBlockExhausted;
    private Iterator<List<PrimitiveValue>> leftBlockIterator;
    private Iterator<List<PrimitiveValue>> rightBlockIterator;
    private Map<String, PrimitiveValue> mergedTuple;
    private Map<String, Integer> schema;


    public JoinOperator(Operator leftChild, Operator rightChild, Join join) {

        this.rightChild = rightChild;
        this.join = join;
        if (join.isSimple()) {
         cacheLeftChild(leftChild);
         setBlockSize();
        } else {
            this.leftChild = leftChild;
        }
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
    public void setChild(String leftOrRight, Operator child){
        if (leftOrRight.equals("left")){
            this.leftChild = child;
        }
        else if (leftOrRight.equals("right")){
            this.rightChild = child;
        }
        else{
            System.out.println("Invalid Child");
        }
    }

    private void setSchema(){
        schema = new LinkedHashMap<String, Integer>();
        Map<String,Integer> leftChildSchema = this.leftChild.getSchema();
        Map<String,Integer> rightChildSchema = this.rightChild.getSchema();

        Set<String> leftColNames = leftChildSchema.keySet();
        Set<String> rightColNames = rightChildSchema.keySet();

        int colCounter = 0;

        for (String leftCol : leftColNames){
            schema.put(leftCol,colCounter);
            colCounter++;
        }

        for (String rightCol : rightColNames){
            if(schema.get(rightCol)==null) {
                schema.put(rightCol, colCounter);
                colCounter++;
            }
        }


    }


    private void cacheLeftChild(Operator leftChild) {

        if (Utils.inMemoryMode) {
            this.leftChild = new InMemoryCacheOperator(leftChild);
        } else {
            if (leftChild instanceof TableScan) {
                this.leftChild = leftChild;
            } else {
                this.leftChild = new OnDiskCacheOperator(leftChild);
            }

        }

    }

    private void setBlockSize() {
        if (Utils.inMemoryMode) {
            this.blockSize = 10;
        } else {
            this.blockSize = 5000;
        }
    }

    public PrimitiveValue eval(Column x) {
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, this.currTuple);
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
            saveSchemaLeftChild();
            saveSchemaRightChild();
        }
        if (leftChildTuple == null || rightChildTuple == null) {
            return null;
        }
        if (join.isSimple()) {
            return simpleJoinNext();
        } else if (join.isNatural()) {
            return naturalJoinNext();
        } else {
            return equiJoinNext();
        }


    }

    private Map<String, PrimitiveValue> simpleJoinNext() {
        return blockNestedLoopJoinNext();
    }


    private Map<String, PrimitiveValue> blockNestedLoopJoinNext() {
        if (isFirstCall) {
            leftBlock = new ArrayList<List<PrimitiveValue>>();
            rightBlock = new ArrayList<List<PrimitiveValue>>();
            isLeftBlockExhausted = false;
            isRightBlockExhausted = false;
            leftBlock.add(Utils.convertToList(leftChildTuple, leftColNameToIdx));
            rightBlock.add(Utils.convertToList(rightChildTuple, rightColNameToIdx));
            loadLeftBlock();
            loadRightBlock();
            isFirstCall = false;
        }
        if (!rightBlockIterator.hasNext() && !leftBlockIterator.hasNext()) {
            isLeftBlockExhausted = true;
            loadLeftBlock();
            if (leftBlock.size() == 0) {
                leftChild.init();
                loadLeftBlock();
                loadRightBlock();
            }
            if (rightBlock.size() == 0) {
                leftBlock.clear();
                rightBlock.clear();
                return null;
            }
            initRightBlockIterator();

        }
        if (!leftBlockIterator.hasNext()) {
            initLeftBlockIterator();
            rightChildTuple = Utils.convertToMap(rightBlockIterator.next(), rightIdxToColName);
        }

        leftChildTuple = Utils.convertToMap(leftBlockIterator.next(), leftIdxToColName);
        if (rightChildTuple == null) {
            leftBlock.clear();
            rightBlock.clear();
            return null;
        }
        return merge(leftChildTuple, rightChildTuple);

    }

    private void initLeftBlockIterator() {
        leftBlockIterator = leftBlock.iterator();
        //leftChildTuple= Utils.convertToMap(leftBlockIterator.next(),leftIdxToColName);
    }

    private void initRightBlockIterator() {
        if (rightBlock.size() != 0) {
            rightBlockIterator = rightBlock.iterator();
            rightChildTuple = Utils.convertToMap(rightBlockIterator.next(), rightIdxToColName);
        }

    }

    private void loadLeftBlock() {
        if (!isFirstCall) leftBlock.clear();
        for (int i = 0; i < blockSize; i++) {
            leftChildTuple = leftChild.next();
            if (leftChildTuple == null) {
                break;
            }
            leftBlock.add(Utils.convertToList(leftChildTuple, leftColNameToIdx));

        }
        initLeftBlockIterator();
    }

    private void loadRightBlock() {
        if (!isFirstCall) {
            rightBlock.clear();
        }
        for (int i = 0; i < blockSize; i++) {

            rightChildTuple = rightChild.next();
            if (rightChildTuple == null) {
                break;
            }
            rightBlock.add(Utils.convertToList(rightChildTuple, rightColNameToIdx));
        }
        initRightBlockIterator();
    }


    private void saveSchemaLeftChild() {
        leftColNameToIdx = new LinkedHashMap<String, Integer>();
        leftIdxToColName = new LinkedHashMap<Integer, String>();
        Utils.fillColIdx(leftChildTuple, leftColNameToIdx, leftIdxToColName);
    }

    private void saveSchemaRightChild() {
        rightColNameToIdx = new LinkedHashMap<String, Integer>();
        rightIdxToColName = new LinkedHashMap<Integer, String>();
        Utils.fillColIdx(rightChildTuple, rightColNameToIdx, rightIdxToColName);
    }

    private Map<String, PrimitiveValue> equiJoinNext() {
        if (isFirstCall) {
            joinColPairs = new ArrayList<List<String>>();
//            rightChildTuple = rightChild.next();
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

    private int compareMaps(Map<String, PrimitiveValue> o1, Map<String, PrimitiveValue> o2) {
        for (List<String> a : joinColPairs) {

            PrimitiveValue value1 = o1.get(a.get(0));
            PrimitiveValue value2 = o2.get(a.get(1));

            if (value1.getType().equals(PrimitiveType.DOUBLE)) {
                try {
                    double val1 = value1.toDouble();
                    double val2 = value2.toDouble();
                    if (val1 < val2) {
                        return -1;
                    } else if (val1 > val2) {
                        return 1;
                    }
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (value1.getType().equals(PrimitiveType.LONG)) {
                try {
                    long val1 = value1.toLong();
                    long val2 = value2.toLong();
                    if (val1 < val2) {
                        return -1;
                    } else if (val1 > val2) {
                        return 1;
                    }
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (value1.getType().equals(PrimitiveType.STRING)) {
                String val1 = value1.toRawString();
                String val2 = value2.toRawString();
                int compare = val1.compareTo(val2);
                if (compare == 0) continue;
                return compare;
                //else return (-1 * compare);

            }
        }
        return 0;
    }

    private Map<String, PrimitiveValue> sortMergeJoinNext() {
        if (isFirstCall) {
            leftOrderByElements = createOrderByElements(0);
            rightOrderByElements = createOrderByElements(1);
//            System.out.println(leftChildTuple);
            orderedLeftChild = new OrderByOperator(leftOrderByElements, leftChild, leftChildTuple);
            orderedRightChild = new OrderByOperator(rightOrderByElements, rightChild, rightChildTuple);
            isFirstCall = false;
        }

        if (endOfChildrenReached & !listsNotExhausted) {
            return null;
        }

        if (listsNotExhausted) {
            return getMergedTupleFromLists();
        } else {
            leftElementsList.clear();
            rightElementsList.clear();
            populateLists();
            return sortMergeJoinNext();
        }

    }

    private Map<String, PrimitiveValue> getMergedTupleFromLists() {
        List<PrimitiveValue> leftElement = null;
        List<PrimitiveValue> rightElement = null;
        if (leftElementsListIterator == null) {
            leftElementsListIterator = leftElementsList.iterator();
            rightElementsListIterator = rightElementsList.iterator();
            currentRightElementInList = rightElementsListIterator.next();
        }
        Map<String, PrimitiveValue> valToBeReturned = null;


        if (!leftElementsListIterator.hasNext()) {
            leftElementsListIterator = leftElementsList.iterator();
            currentRightElementInList = rightElementsListIterator.next();
        }

        leftElement = leftElementsListIterator.next();
        rightElement = currentRightElementInList;

        Map<String, PrimitiveValue> currentLeftTupleInList = Utils.convertToMap(leftElement, leftIdxToColName);
        Map<String, PrimitiveValue> currentRightTupleInList = Utils.convertToMap(rightElement, rightIdxToColName);

        valToBeReturned = merge(currentLeftTupleInList, currentRightTupleInList);

        if (!rightElementsListIterator.hasNext() && !leftElementsListIterator.hasNext()) {
            listsNotExhausted = false;
            leftElementsList.clear();
            rightElementsList.clear();
            leftElementsListIterator = null;
            rightElementsListIterator = null;
        }

        return valToBeReturned;
    }

    private void populateLists() {
        if (currentLeftTuple == null && currentRightTuple == null) {
            currentLeftTuple = orderedLeftChild.next();
            currentRightTuple = orderedRightChild.next();
        }

        if (currentRightTuple == null || currentLeftTuple == null) {
            endOfChildrenReached = true;
            leftElementsList.clear();
            rightElementsList.clear();
            return;
        }
        advanceChildren();
        fillLists();
        listsNotExhausted = true;

    }

    private void fillLists() {
        previousLeftTuple = currentLeftTuple;
        previousRightTuple = currentRightTuple;

        while (orderedLeftChild.compareTuples.compareMaps(previousLeftTuple, currentLeftTuple) == 0) {
            leftElementsList.add(Utils.convertToList(currentLeftTuple, leftColNameToIdx));
            previousLeftTuple = currentLeftTuple;
            currentLeftTuple = orderedLeftChild.next();
            if (currentLeftTuple == null) {
                endOfChildrenReached = true;
                break;
            }
        }

        while (orderedRightChild.compareTuples.compareMaps(previousRightTuple, currentRightTuple) == 0) {
            rightElementsList.add(Utils.convertToList(currentRightTuple, rightColNameToIdx));
            previousRightTuple = currentRightTuple;
            currentRightTuple = orderedRightChild.next();
            if (currentRightTuple == null) {
                endOfChildrenReached = true;
                break;
            }
        }
    }

    private void advanceChildren() {
        while (true) {
            if (compareMaps(currentLeftTuple, currentRightTuple) < 0) {
                currentLeftTuple = orderedLeftChild.next();
            } else if (compareMaps(currentLeftTuple, currentRightTuple) > 0) {
                currentRightTuple = orderedRightChild.next();
            } else {
                break;
            }
        }
    }

    private List<OrderByElement> createOrderByElements(int index) {
        List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
        for (List<String> joinColPair : joinColPairs) {
            OrderByElement orderByElement = new OrderByElement();
            String leftCol = joinColPair.get(index);
            orderByElement.setExpression(getColumn(leftCol));
            orderByElements.add(orderByElement);
        }

        return orderByElements;
    }

    private Column getColumn(String column) {
        String[] tableAndCol = column.split("\\.");
        Column col = null;
        if (tableAndCol.length > 1) {
            Table table = new Table();
            table.setName(tableAndCol[0]);
            String colName = tableAndCol[1];
            col = new Column(table, colName);

        } else {
            String colName = tableAndCol[0];
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
            Map<String, PrimitiveValue> matchedLeftTuple = Utils.convertToMap(matchedLeftTupleSerialized, leftIdxToColName);
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
                tempBucket.add(Utils.convertToList(leftChildTuple, leftColNameToIdx));
            else {
                tempBucket = new ArrayList<List<PrimitiveValue>>();
                tempBucket.add(Utils.convertToList(leftChildTuple, leftColNameToIdx));
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
            for (String rightCol : rightCols) {
                if (Utils.areColsEqual(leftCol, rightCol)) {
                    List<String> colPair = new ArrayList<String>();
                    colPair.add(leftCol);
                    colPair.add(rightCol);
                    joinColPairs.add(colPair);
                }
            }
        }

    }


    public void init() {

    }

    private Map<String, PrimitiveValue> nestedLoopJoin() {
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
        mergedTuple.clear();
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
