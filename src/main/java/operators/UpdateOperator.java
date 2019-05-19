package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.Map;

public class UpdateOperator implements Operator, SingleChildOperator {
    @Override
    public Map<String, PrimitiveValue> next() {
        return null;
    }

    @Override
    public void init() {

    }

    @Override
    public Map<String, Integer> getSchema() {
        return null;
    }

    @Override
    public Operator getChild() {
        return null;
    }

    @Override
    public void setChild(Operator child) {

    }
}
