package operators;

import aggregators.AggregateFactory;
import aggregators.Aggregator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.Utils;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TupleAggWrapper extends Eval {
    Aggregator[] aggregators;
    PrimitiveValue[] tupleArr;
    List<SelectItem> selectItems;
    private Map<String, PrimitiveValue> currTuple;
    Map<String, Integer> schema;
    int aggCnt;

    public TupleAggWrapper(List<SelectItem> selectItems, Map<String, Integer> schema, int aggCnt) {
        tupleArr = new PrimitiveValue[selectItems.size()];
        aggregators = new Aggregator[aggCnt];
        this.selectItems = selectItems;
        this.schema = schema;
        this.aggCnt = aggCnt;
        initializeAggregators(selectItems);
    }

    public PrimitiveValue eval(Function function) throws SQLException {
        String fn = function.getName().toUpperCase();
        if ("DATE".equals(fn)) {
            List args = function.getParameters().getExpressions();
            if (args.size() != 1) {
                throw new SQLException("DATE() takes exactly one argument");
            } else {
                return new DateValue(this.eval((Expression) args.get(0)).toRawString());
            }
        } else if ("SUM".equalsIgnoreCase(fn) || "MIN".equalsIgnoreCase(fn) || "MAX".equalsIgnoreCase(fn) || "AVG".equalsIgnoreCase(fn)) {
            Expression expression = function.getParameters().getExpressions().get(0);
            PrimitiveValue value = eval(expression);
            return value;

        } else if ("COUNT".equalsIgnoreCase(fn)) {

            if (function.isAllColumns() != true) {
                Expression expression = function.getParameters().getExpressions().get(0);
                PrimitiveValue value = eval(expression);
                return value;
            } else {
                return new LongValue(1);
            }

        } else {
            return this.missing("Function:" + fn);
        }
    }


    public void update(Map<String, PrimitiveValue> tuple) {
        this.currTuple = tuple;
        int tupleItr = 0;
        int aggItr = 0;
        for (SelectItem selectItem : selectItems) {
            Expression expression = ((SelectExpressionItem) selectItem).getExpression();
            PrimitiveValue primVal = null;
            try {
                primVal = eval(expression);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (expression instanceof Function) {
                aggregators[aggItr].fold(primVal);
                primVal = aggregators[aggItr].getAggregate();
                aggItr++;
            }
            tupleArr[tupleItr] = primVal;
            tupleItr++;
        }
    }

    private void initializeAggregators(List<SelectItem> selectItems) {
        int i = 0;
        for (SelectItem selectItem : selectItems) {
            Expression expression = ((SelectExpressionItem) selectItem).getExpression();
            if (expression instanceof Function) {
                Function aggFunction = (Function) expression;
                aggregators[i] = AggregateFactory.getAggregator(aggFunction.getName());
                i++;
            }
        }
    }

    public PrimitiveValue eval(Column x) {

        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, currTuple);
    }

    public Map<String, PrimitiveValue> getFinalTuple() {
        Map<String, PrimitiveValue> finalTuple = new LinkedHashMap<String, PrimitiveValue>();
        Set<String> colNames = schema.keySet();
        for (String colName : colNames) {
            PrimitiveValue colVal = tupleArr[schema.get(colName)];
            finalTuple.put(colName, colVal);
        }
        for (int i = 0; i < aggCnt; i++) {
            aggregators[i].init();
        }
        return finalTuple;
    }


}
