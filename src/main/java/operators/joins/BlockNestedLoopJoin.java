package operators.joins;

import net.sf.jsqlparser.expression.PrimitiveValue;
import utils.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BlockNestedLoopJoin implements JoinAlgorithm {
    private List<List<PrimitiveValue>> leftBlock;
    private List<List<PrimitiveValue>> rightBlock;
    private JoinOperator joinOperator;
    boolean isFirstCall;
    private boolean isLeftBlockExhausted;
    private boolean isRightBlockExhausted;
    private Map<String,PrimitiveValue> leftChildTuple;
    private Map<String,PrimitiveValue> rightChildTuple;
    private Iterator<List<PrimitiveValue>> leftBlockIterator;
    private Iterator<List<PrimitiveValue>> rightBlockIterator;
    private int blockSize;
    public BlockNestedLoopJoin(JoinOperator joinOperator){
        this.joinOperator = joinOperator;
        isFirstCall = true;
        leftChildTuple = joinOperator.getLeftChildTuple();
        rightChildTuple = joinOperator.getRightChildTuple();
        setBlockSize();

    }
    private void setBlockSize() {
        if (Utils.inMemoryMode) {
            this.blockSize = 1000000; //TODO change this to a bigger number if you are not adding cache below in memeory join
        } else {
            this.blockSize = 1000;
        }
    }

    public Map<String, PrimitiveValue> getNextValue() {
        if (isFirstCall) {
            leftBlock = new ArrayList<List<PrimitiveValue>>();
            rightBlock = new ArrayList<List<PrimitiveValue>>();
            isLeftBlockExhausted = false;
            isRightBlockExhausted = false;
            leftBlock.add(Utils.convertToList(leftChildTuple, joinOperator.getLeftColNameToIdx()));
            rightBlock.add(Utils.convertToList(rightChildTuple, joinOperator.getRightColNameToIdx()));
            loadLeftBlock();
            loadRightBlock();
            isFirstCall = false;
        }
        if (!rightBlockIterator.hasNext() && !leftBlockIterator.hasNext()) {
            isLeftBlockExhausted = true;
            loadLeftBlock();
            if (leftBlock.size() == 0) {
                joinOperator.getLeftChild().init();
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
            rightChildTuple = Utils.convertToMap(rightBlockIterator.next(), joinOperator.getRightIdxToColName());
        }

        leftChildTuple = Utils.convertToMap(leftBlockIterator.next(), joinOperator.getLeftIdxToColName());
        if (rightChildTuple == null) {
            leftBlock.clear();
            rightBlock.clear();
            return null;
        }
        return joinOperator.merge(leftChildTuple, rightChildTuple);
    }
    private void initLeftBlockIterator() {
        leftBlockIterator = leftBlock.iterator();
    }

    private void initRightBlockIterator() {
        if (rightBlock.size() != 0) {
            rightBlockIterator = rightBlock.iterator();
            rightChildTuple = Utils.convertToMap(rightBlockIterator.next(), joinOperator.getRightIdxToColName());
        }

    }
    private void loadLeftBlock() {
        if (!isFirstCall) leftBlock.clear();
        for (int i = 0; i < blockSize; i++) {
            leftChildTuple = joinOperator.getLeftChild().next();
            if (leftChildTuple == null) {
                break;
            }
            leftBlock.add(Utils.convertToList(leftChildTuple, joinOperator.getLeftColNameToIdx()));

        }
        initLeftBlockIterator();
    }

    private void loadRightBlock() {
        if (!isFirstCall) {
            rightBlock.clear();
        }
        for (int i = 0; i < blockSize; i++) {

            rightChildTuple = joinOperator.getRightChild().next();
            if (rightChildTuple == null) {
                break;
            }
            rightBlock.add(Utils.convertToList(rightChildTuple, joinOperator.getRightColNameToIdx()));
        }
        initRightBlockIterator();
    }


}
