package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.Map;

public interface Operator {
    public Map<String, PrimitiveValue>  next();
    public void init();
}
