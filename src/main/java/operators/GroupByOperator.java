package operators;

import aggregators.*;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.Utils;

import java.sql.SQLException;
import java.util.*;

public class GroupByOperator extends Eval implements Operator,SingleChildOperator {

    List<Column> groupByColumns;

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    List<SelectItem> selectItems;
    Operator child;
    Map<String, List<PrimitiveValue>> groupToTuple;
    Map<String, Aggregator> aliasToAggregator;
    Map<String, Integer> schema;
    int colCnt;
    int aggCnt;
    boolean isFirstCall;
    Map<String, PrimitiveValue> childTuple;
    Map<String, TupleAggWrapper> hashToTuppleAgg;
    Iterator<TupleAggWrapper> tupleAggWrapperIterator;
    Map<String, PrimitiveValue> prevTuple;
    Map<String, PrimitiveValue> currTuple;
    List<OrderByElement> orderByElements;
    private OrderByOperator orderedChild;
    private TupleAggWrapper tupleAggWrapperForSort;
    private String subquery_alias;
    Map<String, PrimitiveValue> finalTuple;

    public GroupByOperator(List<Column> groupByColumns, List<SelectItem> selectItems, Operator operator,String subquery_alias) {
        this.child = operator;
        this.groupByColumns = groupByColumns;
        this.selectItems = selectItems;
        createSchema(selectItems);
        isFirstCall = true;
        this.subquery_alias = subquery_alias;
        this.finalTuple = new LinkedHashMap<String, PrimitiveValue>();
    }

    private void createSchema(List<SelectItem> selectItems) {
        int no_alias_cnt = 0;
        colCnt = 0;
        aggCnt = 0;
        Map<String, Aggregator> aliasToAggregator = new LinkedHashMap<String, Aggregator>();
        schema = new LinkedHashMap<String, Integer>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                String alias = selectExpressionItem.getAlias();
                Expression expression = selectExpressionItem.getExpression();
                if (expression instanceof Function) {
                    aggCnt++;
                }
                if (alias == null) {
                    if (expression instanceof Column) {
                        alias = expression.toString();
                    } else {
                        alias = String.valueOf(no_alias_cnt);
                        no_alias_cnt++;
                    }
                }
                if (subquery_alias != null){
                    alias =  subquery_alias + "." + Utils.getColName(alias);
                }
                schema.put(alias, colCnt);
                colCnt++;
            }
        }

    }

    public Map<String, Integer> getSchema() {
        return schema;
    }

    public PrimitiveValue eval(Column x) {
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, childTuple);
    }

    public Map<String, PrimitiveValue> next() {
        if (Utils.inMemoryMode) {
            return hashGroupByNext();
        } else {
            return sortGroupByNext();
        }
    }

    private Map<String, PrimitiveValue> sortGroupByNext() {
        if (isFirstCall) {
            orderByElements = getOrderByElements();
            orderedChild = new OrderByOperator(orderByElements, child);
            tupleAggWrapperForSort = new TupleAggWrapper(selectItems, schema, aggCnt);
            isFirstCall = false;
            currTuple = orderedChild.next();
            prevTuple = currTuple;
        }
        if (currTuple == null) {
            return null;
        }
        while (orderedChild.getCompareTuples().compareMaps(currTuple, prevTuple) == 0) {
            prevTuple = currTuple;
            currTuple = orderedChild.next();
            if (prevTuple != null) {
                tupleAggWrapperForSort.update(prevTuple);
            }
            if (currTuple == null) {
                break;
            }
        }
        prevTuple = currTuple;
        return tupleAggWrapperForSort.getFinalTuple();
        // TODO change finaltuple of ondiskgroupby

    }

    private List<OrderByElement> getOrderByElements() {
        List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
        for (Column groupByColumn : groupByColumns) {
            OrderByElement orderByElement = new OrderByElement();
            orderByElement.setExpression(groupByColumn);
            orderByElements.add(orderByElement);
        }
        return orderByElements;
    }

    private Map<String, PrimitiveValue> hashGroupByNext() {

        if (!isFirstCall) {
            return emit();
        }
        isFirstCall = false;
        childTuple = child.next();
        hashToTuppleAgg = new HashMap<String, TupleAggWrapper>();
        while (childTuple != null) {
            String groupHash = getGroupHash();
            TupleAggWrapper tupleAggWrapper = hashToTuppleAgg.get(groupHash);
            if (tupleAggWrapper == null) {
                tupleAggWrapper = new TupleAggWrapper(selectItems, schema, aggCnt);
                tupleAggWrapper.update(childTuple);
                hashToTuppleAgg.put(groupHash, tupleAggWrapper);
            } else {
                tupleAggWrapper.update(childTuple);
            }
            childTuple = child.next();
        }
        tupleAggWrapperIterator = hashToTuppleAgg.values().iterator();
        return emit();

    }


//    private Map<String, PrimitiveValue> emit() {
//        if (tupleAggWrapperIterator.hasNext()) {
//            return tupleAggWrapperIterator.next().getFinalTuple();
//        } else {
//            return null;
//        }
//    }

    private Map<String, PrimitiveValue> emit() {
        Iterator<String> colIterator = schema.keySet().iterator();
        int aggCnt=0;
        int colCnt=0;
        if (tupleAggWrapperIterator.hasNext()) {
            TupleAggWrapper tupleAggWrapper = tupleAggWrapperIterator.next();
            for (SelectItem selectItem : selectItems) {
                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                if(expression instanceof Function){
                  finalTuple.put(colIterator.next(),tupleAggWrapper.aggregators[aggCnt].getAggregate());
                  aggCnt++;
                }
                else{
                    finalTuple.put(colIterator.next(),tupleAggWrapper.tupleArr[colCnt]);
                }
                colCnt++;
            }
            return finalTuple;
        } else {
            return null;
        }

    }

    private String getGroupHash() {
        StringJoiner joiner = new StringJoiner(",");
        for (Column groupByColumn : groupByColumns) {
            joiner.add(eval(groupByColumn).toRawString());
        }
        return joiner.toString();
    }


    public void init() {

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

    @Override
    public Operator getChild() {
        return child;
    }

    @Override
    public void setChild(Operator child) {
        this.child = child;
    }
}
