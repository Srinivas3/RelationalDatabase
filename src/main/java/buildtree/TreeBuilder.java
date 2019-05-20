package buildtree;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import operators.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operators.joins.JoinOperator;
import optimizer.QueryOptimizer;
import utils.Utils;

import java.util.List;


public class TreeBuilder {
    QueryOptimizer queryOptimizer;
    public TreeBuilder(){
        queryOptimizer = new QueryOptimizer();
    }

    public Operator buildTree(PlainSelect plainSelect,String alias){
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
            operator = new GroupByOperator(groupByColumns, selectItems, operator,alias);
        }

        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        if(orderByElements!=null && orderByElements.size()!=0){
            operator = new OrderByOperator(orderByElements,operator);
        }

        if(groupByColumns==null || groupByColumns.size()==0){
            operator = new ProjectionOperator(selectItems, operator,alias);
            //queryOptimizer.getOptimizedOperator(operator);
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
    public Operator buildTree(Union union,String alias){
        List<PlainSelect> plainSelects =  union.getPlainSelects();
        Operator prevOperator = buildTree(plainSelects.get(0),alias);
        int i = 1;
        while (i < plainSelects.size()){
            Operator currOperator = buildTree(plainSelects.get(i),alias);
            prevOperator = new UnionOperator(prevOperator,currOperator);
            i++;
        }
        System.out.println("return from build tree union");
        return prevOperator;
    }

    public Operator handleFromItem(FromItem fromItem){
        if (fromItem instanceof Table){
            Table table = (Table)fromItem;
            if(table.getName().startsWith("view")){
                return new TableScan(table.getName());
            }
            else {
                Operator tableScanOperator = new TableScan(table);
                Operator operator = getSelectionOperator(table.getName(),tableScanOperator);
                if(Utils.tableToInserts.get(table.getName())!=null){
                    return new UnionOperator(operator, new InsertOperator(table));
                }else{
                    return operator;
                }

            }

        }
        else if (fromItem instanceof SubSelect){
            SubSelect subselect = (SubSelect) fromItem;
            return handleSelectBody(subselect.getSelectBody(),subselect.getAlias());

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

    private Operator getSelectionOperator(String tableName, Operator tableScanOperator) {
        List<Expression> deleteExpressions = Utils.tableDeleteExpressions.get(tableName);
        if(deleteExpressions == null){
            return tableScanOperator;
        }else{
            Expression whereExp = Utils.constructByAnding(deleteExpressions);
            return new SelectionOperator(whereExp,tableScanOperator);
        }
    }

//    private Operator cloneBaseOperator(Operator baseOperator) {
//        if(baseOperator instanceof UnionOperator){
//            UnionOperator unionOperator = (UnionOperator) baseOperator;
//            Operator leftChild = cloneBaseOperator(unionOperator.getLeftChild());
//            Operator rightChild = cloneBaseOperator(unionOperator.getRightChild());
//            return new UnionOperator(leftChild, rightChild);
//        } else if (baseOperator instanceof SelectionOperator){
//            SelectionOperator selectionOperator = (SelectionOperator) baseOperator;
//            Operator child = cloneBaseOperator(selectionOperator.getChild());
//            Expression whereExp = selectionOperator.getWhereExp();
//            return new SelectionOperator(whereExp, child);
//        } else if (baseOperator instanceof InsertOperator){
//            InsertOperator insertOperator = (InsertOperator) baseOperator;
//            Insert insertStatement = insertOperator.getInsertStatement();
//            return new InsertOperator(insertStatement);
//        } else if (baseOperator instanceof TableScan){
//            TableScan tableScanOperator = (TableScan) baseOperator;
//            return new TableScan(tableScanOperator.getTable());
//        } else if (baseOperator instanceof UpdateOperator){
//            UpdateOperator updateOperator = (UpdateOperator) baseOperator;
//            Operator child = cloneBaseOperator(updateOperator.getChild());
//            return new UpdateOperator(updateOperator.getUpdateStatement(),child);
//        }
//        else {
//            return null;
//        }
//    }

    public Operator handleSelectBody(SelectBody selectBody,String alias){
        if (selectBody instanceof PlainSelect){

            return buildTree((PlainSelect)selectBody,alias);
        }
        else if (selectBody instanceof Union){
            return buildTree((Union)selectBody,alias);
        }
        else{
            return null;
        }
    }

}
