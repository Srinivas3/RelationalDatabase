package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import utils.Utils;

import java.util.Map;

public class SelectionOperator extends Eval implements Operator {
    private Expression whereExp;
    private Operator child;
    Map<String, PrimitiveValue> childTuple;

    public PrimitiveValue eval(Column x) {
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, childTuple);
    }

    public SelectionOperator(Expression whereExp, Operator child) {
        this.child = child;
        this.whereExp = whereExp;
    }

    public Expression getWhereExp() {
        return whereExp;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public void setWhereExp(Expression whereExp) {
        this.whereExp = whereExp;
    }

    public Map<String, PrimitiveValue> next() {
        while ((childTuple = child.next()) != null) {
            BooleanValue whereCond = null;
            try {
                whereCond = (BooleanValue) eval(this.whereExp);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (whereCond.toBool()) {
                return childTuple;
            }


        }
        return null;
    }

    public void init() {
        child.init();
    }

    public Map<String, Integer> getSchema() {
        return child.getSchema();
    }

    public Operator getChild() {
        return child;
    }
}
