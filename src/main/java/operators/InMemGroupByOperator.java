package operators;

import aggregators.*;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import schema.Utils;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMemGroupByOperator extends Eval implements Operator {

    List<Column> columns;
    List<SelectItem> selectItems;
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    Map<String, Map<String,PrimitiveValue>> groupHash;
    Map<String, Map<String, AggregatePattern>> aggregatorsMap = new LinkedHashMap<String, Map<String, AggregatePattern>>();
    Iterator<Map<String,PrimitiveValue>> iterator;

    public InMemGroupByOperator(List<Column> columns,List<SelectItem> selectItems, Operator operator) {
        this.child = operator;
        this.columns = columns;
        this.selectItems = selectItems;
    }

    public Map<String, PrimitiveValue> next() {

        if(groupHash==null){
            groupHash = new LinkedHashMap<String, Map<String,PrimitiveValue>>();
            childTuple= child.next();
            while(childTuple!=null){
                StringBuilder sb = new StringBuilder();
                for(Column x : columns){
                    PrimitiveValue value = eval(x);
                    sb.append(value.toRawString());
                    sb.append("_");
                }
                String key= sb.toString();
                if(groupHash.get(key)!=null){
                    Map<String, PrimitiveValue> oldTuple = groupHash.get(key);
                    for(SelectItem selectItem : selectItems){
                        SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                        if(selectExpressionItem.getExpression() instanceof Function){
                            String alias = selectExpressionItem.getAlias();
                            AggregatePattern aggregator = aggregatorsMap.get(key).get(alias);
                            Expression expression =  selectExpressionItem.getExpression();
                            PrimitiveValue value = null;
                            try {
                                value = eval(expression);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                            aggregator.fold(value);
                            oldTuple.put(alias,aggregator.getAggregate());
                        }

                    }
                }
                else {
                    Map<String, AggregatePattern> aggregators= new LinkedHashMap<String, AggregatePattern>();
                    groupHash.put(key,compAggregate(aggregators));
                    aggregatorsMap.put(key,aggregators);
                }
                childTuple = child.next();
            }
            iterator = groupHash.values().iterator();
        }
        if(iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    public Map<String,PrimitiveValue> compAggregate(Map<String, AggregatePattern> aggregators){
        Map<String, PrimitiveValue> tuple = new LinkedHashMap<String, PrimitiveValue>();
        int count=0;
        for(SelectItem selectItem : selectItems){
            count++;
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
            String alias = selectExpressionItem.getAlias();
            if (alias == null){
                selectExpressionItem.setAlias(Integer.toString(count)+ "_" + selectExpressionItem.toString());
                alias = selectExpressionItem.getAlias();
            }

            Expression expression =  selectExpressionItem.getExpression();
            PrimitiveValue value = null;
            try {
                value = eval(expression);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            if(expression instanceof Function){
                Function function = (Function) expression;
                String str = function.getName();
                handleAggregator(aggregators, alias, value, function);
            }

        }
        for(SelectItem selectItem : selectItems){

            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
            if(selectExpressionItem.getExpression() instanceof Function){
                String alias = selectExpressionItem.getAlias();
                PrimitiveValue finalValue = aggregators.get(alias).getAggregate();
                tuple.put(alias,finalValue);
            }
            else {
                Expression expression =  selectExpressionItem.getExpression();
                String alias = selectExpressionItem.getAlias();
                if (alias == null){
                    selectExpressionItem.setAlias(Integer.toString(count)+ "_" + selectExpressionItem.toString());
                    alias = selectExpressionItem.getAlias();
                }
                PrimitiveValue value = null;
                try {
                    value = eval(expression);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                tuple.put(alias,value);

            }

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

    public void init() {

    }

    public PrimitiveValue eval(Column x){

        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, childTuple);
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
