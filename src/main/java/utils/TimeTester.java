package utils;

import CustomPrimitiveTypes.EfficientDateValue;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;

public class TimeTester extends Eval {
    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        return null;
    }

    public void timeTester() {
        firstScenario();
        secondScenario();
    }

    private void firstScenario() {
        long t1 = System.currentTimeMillis();
        int i = 0;
        for (i = 0; i < 100000000; i++) {
            PrimitiveValue dateValue = new DateValue("1995-01-01");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("time for first scenario " + String.valueOf(t2 - t1));
    }

    private void secondScenario() {
        long t1 = System.currentTimeMillis();
        int i;
        for (i = 0; i < 100000000; i++) {
            short year = 1995;
            byte month = 1;
            byte day = 1;
            PrimitiveValue efficientDateValue = new EfficientDateValue(year,month,day);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("time for second scenario " + String.valueOf(t2 - t1));

    }

}

