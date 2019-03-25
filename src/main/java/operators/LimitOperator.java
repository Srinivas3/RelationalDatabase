package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Limit;

import java.util.Map;

public class LimitOperator implements Operator {
    long rowCount = 0;
    long offset = 0;
    Operator child;
    Map<String,PrimitiveValue> childTuple;

    public LimitOperator(Limit limit, Operator operator) {
        rowCount = limit.getRowCount();
        offset = limit.getOffset();
        child = operator;
    }

    public Map<String, PrimitiveValue> next() {
        childTuple = child.next();
        while ( childTuple != null && offset > 0) {
            childTuple = child.next();
            offset--;
        }

        if(childTuple != null && rowCount > 0){
            rowCount--;
            return childTuple;
        }


        return null;
    }

    public void init() {

    }
}
