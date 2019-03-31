package aggregators;

import net.sf.jsqlparser.expression.PrimitiveValue;

public interface Aggregator {

    void init();

    void fold(PrimitiveValue next);

    PrimitiveValue getAggregate();

}
