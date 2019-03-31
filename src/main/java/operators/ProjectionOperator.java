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
    boolean isAggregate = false;
    boolean isFirstCall = true;
    private int colCounter;
    private Map<String, Integer> schema;
    int no_alias_cnt;


    public ProjectionOperator(List<SelectItem> selectItems, Operator child){

        this.child = child;
        this.selectItems = selectItems;
        setSchema();
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

    public Map<String, Integer> getSchema() {
        return schema;
    }
    private String getAlias(SelectExpressionItem selectExpressionItem){
        String alias = selectExpressionItem.getAlias();
        if(alias==null){
            Expression expression = selectExpressionItem.getExpression();
            if(expression instanceof Column){
                alias = expression.toString();
            }else{
                alias = expression.toString()+"_"+no_alias_cnt;
                no_alias_cnt++;
            }
        }
        return alias;

    }

    private void setSchema(){
        schema = new LinkedHashMap<String, Integer>();
        Map<String,Integer> childSchema = this.child.getSchema();
        Set<String> colNames = childSchema.keySet();
        no_alias_cnt = 0;
        for(SelectItem selectItem : selectItems){
            if(selectItem instanceof SelectExpressionItem){
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                String alias = getAlias(selectExpressionItem);
                schema.put(alias,colCounter);
                colCounter++;
            } else if(selectItem instanceof AllColumns){
                schema = childSchema;
                return;
            } else if(selectItem instanceof AllTableColumns){
                String tableName = selectItem.toString().split("\\.")[0];
                allColsFromChild(tableName,colNames);
            }
        }
    }

    private void allColsFromChild(String table,Set<String> colNames){
        for(String colName : colNames){
            if(Utils.isSameTable(table,colName)){
                schema.put(colName,colCounter);
                colCounter++;
            }
        }
    }

    private Map<String, PrimitiveValue> aggregateNext(){
        childTuple = child.next();
        if (childTuple == null)
            return null;

        Map<String, Aggregator> aggregators= new LinkedHashMap<String, Aggregator>();
        Map<String,Function> aggFunctions = new LinkedHashMap<String,Function>();
        no_alias_cnt = 0;
        for (SelectItem selectItem: selectItems){
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
            String alias = getAlias(selectExpressionItem);
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
        no_alias_cnt = 0;
        for(SelectItem selectItem : selectItems){
            if (selectItem instanceof SelectExpressionItem){
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                String alias = getAlias(selectExpressionItem);
                Expression expression = selectExpressionItem.getExpression();
                PrimitiveValue primVal = null;
                try {
                    primVal = eval(expression);
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

    public Operator getChild() {
        return child;
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

}
