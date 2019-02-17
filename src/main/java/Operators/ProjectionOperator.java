package Operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import schema.TableUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProjectionOperator extends Eval implements Operator{
    List<SelectItem> selectItems;
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    String defaultAlias;
    public ProjectionOperator(List<SelectItem> selectItems, Operator child){
        this.child = child;
        this.selectItems = selectItems;
    }
    public PrimitiveValue eval(Column x){

        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        this.defaultAlias = colName;
        return TableUtils.getColValue(tableName,colName,childTuple);
    }
    public Map<String,PrimitiveValue> next(){
        this.childTuple = child.next();
        if (childTuple == null){
            return null;
        }
        Map<String,PrimitiveValue> tuple = new LinkedHashMap<String,PrimitiveValue>();
        for(SelectItem selectItem : selectItems){
            if (selectItem instanceof SelectExpressionItem){
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
                String alias = selectExpressionItem.getAlias();
                if (alias == null)
                    alias = this.defaultAlias;
                Expression expression = selectExpressionItem.getExpression();
                PrimitiveValue primVal = null;
                try {
                    primVal = eval(expression);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                tuple.put(alias,primVal);
            }
            else if (selectItem instanceof AllColumns){
                return childTuple;
            }
            else{
                return null;
            }
        }

        return tuple;

    }
    public void init(){

    }
}
