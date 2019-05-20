package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import utils.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SelectionOperator extends Eval implements Operator,SingleChildOperator {
    private Expression whereExp;
    private Operator child;
    Map<String, PrimitiveValue> childTuple;
    List<Expression> andExpressions;

    public PrimitiveValue eval(Column x) {
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, childTuple);
    }

    public SelectionOperator(Expression whereExp, Operator child) {
        this.child = child;
        this.whereExp = whereExp;
        andExpressions = new ArrayList<Expression>();
        Utils.populateAndExpressions(this.whereExp, andExpressions);

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
            boolean whereCond = evaluateWhereExpressions();
            if (whereCond) {
                return childTuple;
            }
        }
        return null;
    }

    private boolean evaluateWhereExpressions(){

        for (Expression andExpression : andExpressions){

            BooleanValue whereCond = null;
            try {
                whereCond = (BooleanValue) eval(andExpression);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (whereCond.toBool() == false) {
                return false;
            }

        }

        return true;
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
