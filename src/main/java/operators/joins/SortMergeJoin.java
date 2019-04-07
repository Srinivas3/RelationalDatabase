package operators.joins;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import operators.Operator;
import operators.OrderByOperator;
import utils.Utils;

import java.util.*;

public class SortMergeJoin implements JoinAlgorithm{
    private JoinOperator joinOperator;
    private EquiJoinUtils equiJoinUtils;
    private boolean isFirstCall;
    private Operator leftChild;
    private Operator rightChild;
    private Map<String,PrimitiveValue> leftChildTuple;
    private Map<String,PrimitiveValue> rightChildTuple;
    private Iterator<List<PrimitiveValue>> leftElementsListIterator = null;
    private Iterator<List<PrimitiveValue>> rightElementsListIterator = null;
    private OrderByOperator orderedLeftChild = null;
    private OrderByOperator orderedRightChild = null;
    boolean lastMovedOnRight = true;
    boolean iterateOverChildren = true;
    private boolean listsNotExhausted = false;
    private boolean endOfChildrenReached = false;
    private List<OrderByElement> leftOrderByElements = null;
    private List<OrderByElement> rightOrderByElements = null;
    private List<List<PrimitiveValue>> leftElementsList = new ArrayList<List<PrimitiveValue>>();
    private List<List<PrimitiveValue>> rightElementsList = new ArrayList<List<PrimitiveValue>>();
    private Map<String, PrimitiveValue> currentLeftTuple = null;
    private Map<String, PrimitiveValue> currentRightTuple = null;
    private List<PrimitiveValue> currentRightElementInList = null;

    private Map<String, PrimitiveValue> previousLeftTuple = null;
    private Map<String, PrimitiveValue> previousRightTuple = null;


    public SortMergeJoin(JoinOperator joinOperator){
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
            leftOrderByElements = createOrderByElements(0);
            rightOrderByElements = createOrderByElements(1);
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
            return getNextValue();
        }

    }
    private Map<String, PrimitiveValue> getMergedTupleFromLists() {
        List<PrimitiveValue> leftElement = null;
        List<PrimitiveValue> rightElement = null;
        if (leftElementsListIterator == null) {
            leftElementsListIterator = leftElementsList.iterator();
            rightElementsListIterator = rightElementsList.iterator();
            if (rightElementsListIterator.hasNext()) {
                currentRightElementInList = rightElementsListIterator.next();
            } else {
                return null;
            }
        }
        Map<String, PrimitiveValue> valToBeReturned = null;


        if (!leftElementsListIterator.hasNext()) {
            leftElementsListIterator = leftElementsList.iterator();
            currentRightElementInList = rightElementsListIterator.next();
        }

        leftElement = leftElementsListIterator.next();
        rightElement = currentRightElementInList;

        Map<String, PrimitiveValue> currentLeftTupleInList = Utils.convertToMap(leftElement, joinOperator.getLeftIdxToColName());
        Map<String, PrimitiveValue> currentRightTupleInList = Utils.convertToMap(rightElement, joinOperator.getRightIdxToColName());

        valToBeReturned = joinOperator.merge(currentLeftTupleInList, currentRightTupleInList);

        if (!rightElementsListIterator.hasNext() && !leftElementsListIterator.hasNext()) {
            listsNotExhausted = false;
            leftElementsList.clear();
            rightElementsList.clear();
            leftElementsListIterator = null;
            rightElementsListIterator = null;
        }

        return valToBeReturned;
    }

    private List<OrderByElement> createOrderByElements(int index) {
        List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
        for (List<String> joinColPair : equiJoinUtils.getJoinColPairs()) {
            OrderByElement orderByElement = new OrderByElement();
            String leftCol = joinColPair.get(index);
            orderByElement.setExpression(getColumn(leftCol));
            orderByElements.add(orderByElement);
        }

        return orderByElements;
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
    private void advanceChildren() {
        while (true) {
            if (currentLeftTuple == null || currentRightTuple == null) {
                break;
            }
            if (compareMaps(currentLeftTuple, currentRightTuple) < 0) {
                currentLeftTuple = orderedLeftChild.next();
            } else if (compareMaps(currentLeftTuple, currentRightTuple) > 0) {
                currentRightTuple = orderedRightChild.next();
            } else {
                break;
            }
        }
    }
    private void fillLists() {
        previousLeftTuple = currentLeftTuple;
        previousRightTuple = currentRightTuple;
        if (currentRightTuple == null || currentLeftTuple == null) {
            endOfChildrenReached = true;
            leftElementsList.clear();
            rightElementsList.clear();
            return;
        }

        while (orderedLeftChild.getCompareTuples().compareMaps(previousLeftTuple, currentLeftTuple) == 0) {
            leftElementsList.add(Utils.convertToList(currentLeftTuple, joinOperator.getLeftColNameToIdx()));
            previousLeftTuple = currentLeftTuple;
            currentLeftTuple = orderedLeftChild.next();
            if (currentLeftTuple == null) {
                endOfChildrenReached = true;
                break;
            }
        }

        while (orderedRightChild.getCompareTuples().compareMaps(previousRightTuple, currentRightTuple) == 0) {
            rightElementsList.add(Utils.convertToList(currentRightTuple, joinOperator.getRightColNameToIdx()));
            previousRightTuple = currentRightTuple;
            currentRightTuple = orderedRightChild.next();
            if (currentRightTuple == null) {
                endOfChildrenReached = true;
                break;
            }
        }
    }

    private int compareMaps(Map<String, PrimitiveValue> o1, Map<String, PrimitiveValue> o2) {

        for (List<String> a : equiJoinUtils.getJoinColPairs()) {

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


}
