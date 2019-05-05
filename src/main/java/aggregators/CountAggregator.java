package aggregators;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class CountAggregator implements Aggregator {
    PrimitiveValue accum;
    long cntVal = 0;

    void CountAggregator(){
        init();
        cntVal = 0;
    }

    public void init() {
        accum = null;
    }


    public void fold(PrimitiveValue next) {

        cntVal++;
    }

    public PrimitiveValue getAggregate() {
        //return new LongValue(cntVal);
        return new LongValue(cntVal);
    }
}
