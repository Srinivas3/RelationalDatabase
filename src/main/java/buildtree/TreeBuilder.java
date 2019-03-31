package buildtree;

import net.sf.jsqlparser.schema.Column;
import operators.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import optimizer.QueryOptimizer;

import java.util.List;


public class TreeBuilder {
    QueryOptimizer queryOptimizer;
    public TreeBuilder(){
        queryOptimizer = new QueryOptimizer();
    }

    public Operator buildTree(PlainSelect plainSelect){
        FromItem fromItem = plainSelect.getFromItem();

        Operator operator = handleFromItem(fromItem);

        List<Join> joins = plainSelect.getJoins();
        if (joins != null){
            for (Join join: joins)
                operator = handleJoin(operator,join);
        }

        Expression expression = plainSelect.getWhere();
        if (expression != null) {
            operator = new SelectionOperator(expression, operator);
            operator = queryOptimizer.getOptimizedOperator(operator);
        }
        List<Column> groupByColumns = plainSelect.getGroupByColumnReferences();
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        if(groupByColumns!=null && groupByColumns.size()!=0){
            operator = new GroupByOperator(groupByColumns, selectItems, operator);
        }

        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        if(orderByElements!=null && orderByElements.size()!=0){
            operator = new OrderByOperator(orderByElements,operator);
        }

        if(groupByColumns==null || groupByColumns.size()==0){
            operator = new ProjectionOperator(selectItems, operator);
        }

        if (plainSelect.getLimit() != null){
            operator = new LimitOperator(plainSelect.getLimit(), operator);
        }
        return operator;
    }
    public Operator handleJoin(Operator leftOperator, Join join){
        FromItem rightFromItem = join.getRightItem();
        Operator rightOperator = handleFromItem(rightFromItem);
        return new JoinOperator(leftOperator,rightOperator,join);
    }
    public Operator buildTree(Union union){
        List<PlainSelect> plainSelects =  union.getPlainSelects();
        Operator prevOperator = buildTree(plainSelects.get(0));
        int i = 1;
        while (i < plainSelects.size()){
            Operator currOperator = buildTree(plainSelects.get(i));
            prevOperator = new UnionOperator(prevOperator,currOperator);
            i++;
        }
        System.out.println("return from build tree union");
        return prevOperator;
    }

    public Operator handleFromItem(FromItem fromItem){
        if (fromItem instanceof Table){
            return new TableScan((Table)fromItem);
        }
        else if (fromItem instanceof SubSelect){
            SubSelect subselect = (SubSelect) fromItem;
            return handleSelectBody(subselect.getSelectBody());

        }
        else if (fromItem instanceof SubJoin){
            SubJoin subJoin = (SubJoin)fromItem;
            Operator leftOperator = handleFromItem(subJoin.getLeft());
            return handleJoin(leftOperator,subJoin.getJoin());
        }
        else{
            return null;
        }

    }

    public Operator handleSelectBody(SelectBody selectBody){
        if (selectBody instanceof PlainSelect){

            return buildTree((PlainSelect)selectBody);
        }
        else if (selectBody instanceof Union){
            return buildTree((Union)selectBody);
        }
        else{
            return null;
        }
    }

}
