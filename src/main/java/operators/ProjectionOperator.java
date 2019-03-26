package operators;

import aggregators.*;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import schema.TableUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProjectionOperator extends Eval implements Operator{
    List<SelectItem> selectItems;
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    String defaultAlias;
    boolean isAggregate = false;

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
        return TableUtils.getColValue(tableName, colName, childTuple);
    }
    public Map<String, PrimitiveValue> next(){

        if (!isAggregate){
            return volcanoNext();
        } else {
            isAggregate=false;
            return aggregateNext();
        }


    }

    private Map<String, PrimitiveValue> aggregateNext(){

        AggregatePattern aggregator = null;
        Map<String, AggregatePattern> aggregators= new LinkedHashMap<String, AggregatePattern>();
        Map<String, PrimitiveValue> tuple = new LinkedHashMap<String, PrimitiveValue>();

        while ((this.childTuple = child.next()) != null ){
            int count=0;
            for(SelectItem selectItem : selectItems){
                count++;
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                String alias = selectExpressionItem.getAlias();
                if (alias == null){
                    selectExpressionItem.setAlias(selectExpressionItem.toString() + "_" + Integer.toString(count));
                    alias = selectExpressionItem.getAlias();
                }

                Expression expression =  selectExpressionItem.getExpression();
                PrimitiveValue value = null;
                try {
                    value = eval(expression);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                Function function = (Function) expression;
                String str = function.getName();
                handleAggregator(aggregators, alias, value, function);
            }
        }

        for(SelectItem selectItem : selectItems){

            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
            String alias = selectExpressionItem.getAlias();
            PrimitiveValue finalValue = aggregators.get(alias).getAggregate();
            tuple.put(alias,finalValue);
        }

        return tuple;
    }

    private void handleAggregator(Map<String, AggregatePattern> aggregators, String alias, PrimitiveValue value, Function function) {
        if (function.getName().equalsIgnoreCase("SUM")){
            if(aggregators.get(alias)==null){
                AggregatePattern sumAggregator = new SumAggregator();
                aggregators.put(alias, sumAggregator);
            }
        } else if(function.getName().equalsIgnoreCase("COUNT")){
            if(aggregators.get(alias)==null){
                AggregatePattern countAggregator = new CountAggregator();
                aggregators.put(alias, countAggregator);
            }
        } else if(function.getName().equalsIgnoreCase("MIN")){
            if(aggregators.get(alias)==null){
                AggregatePattern minAggregator = new MinAggregator();
                aggregators.put(alias, minAggregator);
            }
        } else if(function.getName().equalsIgnoreCase("MAX")){
            if(aggregators.get(alias)==null){
                AggregatePattern maxAggregator = new MaxAggregator();
                aggregators.put(alias, maxAggregator);
            }
        } else if(function.getName().equalsIgnoreCase("AVG")){
            if(aggregators.get(alias)==null){
                AggregatePattern avgAggregator = new AverageAggregator();
                aggregators.put(alias, avgAggregator);
            }
        }
        aggregators.get(alias).fold(value);

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
