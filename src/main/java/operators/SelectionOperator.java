package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import schema.TableUtils;

import java.util.Map;

public class SelectionOperator extends Eval implements Operator {
    Expression expression;
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    public PrimitiveValue eval(Column x) {
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return TableUtils.getColValue(tableName, colName, childTuple);
    }
    public SelectionOperator(Expression expression,Operator child){
        this.child = child;
        this.expression = expression;
    }

    public Map<String, PrimitiveValue> next(){
        while ( (childTuple = child.next())!= null) {
            BooleanValue whereCond = null;
            try {
                whereCond = (BooleanValue) eval(this.expression);
            }
            catch (Exception e){
                e.printStackTrace();
            }
            if (whereCond.toBool()){
                return childTuple;
            }


        }
        return null;
    }
    public void init(){

    }
}
