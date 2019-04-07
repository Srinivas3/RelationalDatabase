package operators.joins;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.util.*;
public interface JoinAlgorithm {
    Map<String, PrimitiveValue> getNextValue();
}
