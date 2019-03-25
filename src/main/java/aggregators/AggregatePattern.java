package aggregators;

import net.sf.jsqlparser.expression.PrimitiveValue;

public interface AggregatePattern {

    void init();

    void fold(PrimitiveValue next);

    PrimitiveValue getAggregate();

}
