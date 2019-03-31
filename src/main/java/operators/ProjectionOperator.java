package operators;

import aggregators.*;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import schema.Utils;

import java.sql.SQLException;
import java.util.*;

public class ProjectionOperator extends Eval implements Operator{
    List<SelectItem> selectItems;
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    String defaultAlias;
    boolean isAggregate = false;
    boolean isFirstCall = true;

    public PrimitiveValue eval(Function function) throws SQLException {
        String fn = function.getName().toUpperCase();
        if ("DATE".equals(fn)) {
            List args = function.getParameters().getExpressions();
            if (args.size() != 1) {
                throw new SQLException("DATE() takes exactly one argument");
            } else {
                return new DateValue(this.eval((Expression)args.get(0)).toRawString());
            }
        } else if ("SUM".equalsIgnoreCase(fn) || "MIN".equalsIgnoreCase(fn) || "MAX".equalsIgnoreCase(fn) || "AVG".equalsIgnoreCase(fn)){
            Expression expression = function.getParameters().getExpressions().get(0);
            PrimitiveValue value = eval(expression);
            return value;

        } else if ("COUNT".equalsIgnoreCase(fn)){

            if(function.isAllColumns()!=true){
                Expression expression = function.getParameters().getExpressions().get(0);
                PrimitiveValue value = eval(expression);
                return value;
            }else {
                return new LongValue(1);
            }

        } else {
            return this.missing("Function:" + fn);
        }
    }

    public ProjectionOperator(List<SelectItem> selectItems, Operator child){

        this.child = child;
        this.selectItems = selectItems;

        //TODO recursively check select expression for expression of type Function(EX:SUM(A) + AVG(B))
        for (SelectItem selectItem : selectItems){
            if (selectItem instanceof SelectExpressionItem){
                Expression expression =  ((SelectExpressionItem) selectItem).getExpression();
                if (expression instanceof Function){
                    isAggregate = true;
                    break;
                }
            }
        }
    }

    public PrimitiveValue eval(Column x){

        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        this.defaultAlias = x.toString();
        return Utils.getColValue(tableName, colName, childTuple);
    }
    public Map<String, PrimitiveValue> next(){
        if (isAggregate){
            if (isFirstCall){
                isFirstCall = false;
                return aggregateNext();
            }
            else
                return null;
        }
        else{
            return volcanoNext();
        }
    }

    private Map<String, PrimitiveValue> aggregateNext(){
        childTuple = child.next();
        if (childTuple == null)
            return null;

        Map<String, Aggregator> aggregators= new LinkedHashMap<String, Aggregator>();
        Map<String,Function> aggFunctions = new LinkedHashMap<String,Function>();
        int count = 0;
        for (SelectItem selectItem: selectItems){
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
            String alias = selectExpressionItem.getAlias();
            if (alias == null){
                alias = selectExpressionItem.toString() + "_" + Integer.toString(count);
                count++;
            }
            Function functionExp = (Function)selectExpressionItem.getExpression();
            Aggregator aggregator = AggregateFactory.getAggregator(functionExp.getName());
            aggFunctions.put(alias,functionExp);
            aggregators.put(alias,aggregator);
        }
        Set<String> aliases = aggregators.keySet();
        while (childTuple != null){
            for (String alias: aliases){
                PrimitiveValue value = null;
                try {
                    value = eval(aggFunctions.get(alias));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                aggregators.get(alias).fold(value);
            }
            childTuple = child.next();
        }
        Map<String,PrimitiveValue> outTuple = new LinkedHashMap<String,PrimitiveValue>();
        for (String alias: aliases){
                outTuple.put(alias,aggregators.get(alias).getAggregate());
        }
        return outTuple;

    }

    private Map<String, PrimitiveValue> volcanoNext() {
        this.childTuple = child.next();
        if (childTuple == null){
            return null;
        }
        Map<String,PrimitiveValue> tuple = new LinkedHashMap<String,PrimitiveValue>();
        for(SelectItem selectItem : selectItems){
            if (selectItem instanceof SelectExpressionItem){
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                String alias = selectExpressionItem.getAlias();
                Expression expression = selectExpressionItem.getExpression();
                PrimitiveValue primVal = null;
                try {
                    primVal = eval(expression);
                    if (alias == null)
                        alias = this.defaultAlias;
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                tuple.put(alias, primVal);
            }
            else if (selectItem instanceof AllColumns)
                return childTuple;
            else if (selectItem instanceof AllTableColumns)
                insertAllCols(tuple, (AllTableColumns) selectItem);
            else
                return null;

        }

        return tuple;
    }

    public void init(){
        child.init();
    }
    private void insertAllCols(Map<String,PrimitiveValue> tuple,AllTableColumns allTableColumns){
        String tableName = allTableColumns.getTable().getName();
        for (String key : childTuple.keySet()){
            String keyTableName = key.split("\\.")[0];
            if (keyTableName.equals(tableName))
                tuple.put(key,childTuple.get(key));
        }


    }
}
