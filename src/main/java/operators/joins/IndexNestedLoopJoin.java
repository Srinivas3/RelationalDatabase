package operators.joins;

import net.sf.jsqlparser.expression.PrimitiveValue;
import operators.Operator;
import operators.TableScan;

import java.util.Map;

public class IndexNestedLoopJoin implements JoinAlgorithm {
    private final JoinOperator joinOperator;
    private final Operator otherOperator;
    private IndexScanOperator indexScan;
    boolean isFirstCall;
    private Map<String, PrimitiveValue> otherTuple;
    private Map<String, PrimitiveValue> indexTuple;
    private String otherColName;

    public IndexNestedLoopJoin(JoinOperator joinOperator, IndexScanOperator indexScan, Operator otherOperator, String otherColName) {
        this.indexScan = indexScan;
        this.joinOperator = joinOperator;
        this.otherOperator = otherOperator;
        this.otherColName = otherColName;
        isFirstCall = true;
    }

    public Map<String, PrimitiveValue> getNextValue() {
        if (indexTuple == null) {
            otherTuple = otherOperator.next();
            if (otherTuple == null) {
                return null;
            }
            PrimitiveValue primitiveValue = otherTuple.get(otherColName);
            indexScan.setPrimitiveValue(primitiveValue);
            indexScan.init();
        }
        indexTuple = indexScan.next();
        if (indexTuple != null) {
            return joinOperator.merge(otherTuple, indexTuple);
        } else {
            return getNextValue();
        }
    }
}
