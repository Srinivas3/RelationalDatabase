package utils;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.Comparator;

public class PrimValComp extends Eval implements Comparator<PrimitiveValue> {
    public int compare(PrimitiveValue o1, PrimitiveValue o2) {
        if (o1 instanceof StringValue){
            return o1.toRawString().compareTo(o2.toRawString());
        }
        GreaterThan greaterThan = new GreaterThan(o1, o2);
        EqualsTo equalsTo = new EqualsTo(o1, o2);
        try {
            boolean isGreaterThan = eval(greaterThan).toBool();
            if (isGreaterThan) {
                return 1;
            }
            boolean isEqualTo = eval(equalsTo).toBool();
            if (isEqualTo) {
                return 0;
            }


        } catch (Exception e) {

        }
        return -1;
    }


    public PrimitiveValue eval(Column column) throws SQLException {
        return null;
    }
}
