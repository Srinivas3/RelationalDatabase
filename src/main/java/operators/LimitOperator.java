package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Limit;

import java.util.Map;

public class LimitOperator implements Operator {
    long rowCount = 0;
    long offset = 0;
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    Map<String,Integer> schema;
    public LimitOperator(Limit limit, Operator child) {
        rowCount = limit.getRowCount();
        offset = limit.getOffset();
        this.child = child;
        this.schema = child.getSchema();
    }

    public Map<String, Integer> getSchema() {
        return schema;
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
