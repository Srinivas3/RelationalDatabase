package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.expression.*;
import operators.*;
import buildtree.TreeBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import operators.joins.JoinOperator;
import optimizer.QueryOptimizer;
import preCompute.PreComputeLoader;
import preCompute.PreProcessor;
import preCompute.ViewBuilder;
import utils.Constants;
import utils.TimeTester;
import utils.Utils;

public class Main {
    private static boolean is_testMode = false;
    private static boolean isPhaseOne = false;
    private static boolean isFirstSelect = true;
    private static boolean areDirsCreated = false;


    public static void main(String args[]) throws ParseException, FileNotFoundException {
        //  new TimeTester().timeTester();
        Utils.populateRangeScanData();
        long time1 = 0;
        long time2 = 0;
        try {
            if (args.length > 0) {
                if (args[0].equalsIgnoreCase("--in-mem")) {
                    Utils.inMemoryMode = true;
                } else {
                    Utils.inMemoryMode = false;
                }
            } else {
                Utils.inMemoryMode = true;
            }
            System.out.println("$>");
            CCJSqlParser parser = new CCJSqlParser(System.in);
            Statement statement;
            List<Statement> statements = new ArrayList<Statement>();
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(System.out));
            int createStmnts = 0;
            while ((statement = parser.Statement()) != null) {
                if (statement instanceof Select) {

                    if (!isPhaseOne && isFirstSelect) {
                        PreComputeLoader preComputeLoader = new PreComputeLoader();
                        preComputeLoader.loadSavedState();
                        isFirstSelect = false;
                    }
                    long startTime = System.currentTimeMillis();
                    Operator root = handleSelect((Select) statement);
                    QueryOptimizer queryOptimizer = new QueryOptimizer();
                     root = queryOptimizer.replaceWithSelectionViews(root);
                    queryOptimizer.projectionPushdown(root);
                    displayOutput(root, bufferedWriter);
//                    printCacheState(bufferedWriter);
                    long endTime = System.currentTimeMillis();
//                    bufferedWriter.write("Execution time for query " + String.valueOf(endTime - startTime));
                    bufferedWriter.flush();
                } else if (statement instanceof CreateTable) {
                    if (!areDirsCreated) {
                        createDirs();
                        areDirsCreated = true;
                    }
                    CreateTable createTableStatement = (CreateTable) statement;
                    PreProcessor preProcessor = new PreProcessor(createTableStatement);
                    preProcessor.preCompute();
                    isPhaseOne = true;
                    createStmnts++;
                    if (createStmnts == 8) {
                        new ViewBuilder().buildViews();
                        for (String key : Utils.cachedCols.keySet()){
                            System.out.println(key);
                        }
                    }
                } else {
                    bufferedWriter.write("Invalid Query");
                }
                bufferedWriter.write("$>" + "\n");
                bufferedWriter.flush();
            }
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printViewExps() {
        Set<String> viewNames = Utils.viewToExpression.keySet();
        for (String viewName : viewNames) {
            System.out.println("viewName: " + viewName);
            System.out.println("View Expression: " + Utils.viewToExpression.get(viewName).toString());
        }
    }

    private static void printStatements(List<Statement> statements, BufferedWriter bufferedWriter) {
        try {
            bufferedWriter.write("Phase#1 statements:");
            bufferedWriter.newLine();
            File phaseOneStatsFile = new File(Constants.PHASE_ONE_STATEMENTS_FILE);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(phaseOneStatsFile));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
            for (Statement statement : statements) {
                bufferedWriter.write(statements.toString());
                bufferedWriter.newLine();
            }
        } catch (Exception e) {

        }

    }

    private static void saveStatements(List<Statement> statements) {
        File statmentsFile = new File(Constants.PHASE_ONE_STATEMENTS_FILE);
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(statmentsFile));
            for (Statement statement : statements) {
                bufferedWriter.write(statement.toString());
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void printCacheState(BufferedWriter bufferedWriter) throws Exception {
        bufferedWriter.write("Start printing cache state");
        bufferedWriter.newLine();
        for (String tableColName : Utils.cachedCols.keySet()) {
            bufferedWriter.write(tableColName + " number of primitive values " + Utils.cachedCols.get(tableColName).size());
            bufferedWriter.newLine();
        }
        bufferedWriter.write("finished cache state");
        bufferedWriter.newLine();
    }

    private static void createDirs() {
        new File(Constants.COL_DEFS_DIR).mkdirs();
        new File(Constants.LINES_DIR).mkdirs();
        new File(Constants.COMPRESSED_TABLES_DIR).mkdirs();
        new File(Constants.COLUMN_STORE_DIR).mkdirs();
        new File(Constants.COLUMN_BYTES_DIR).mkdirs();
        new File(Constants.VIEW_SCHEMA_DIR).mkdirs();
        new File(Constants.VIEW_EXPS_DIR).mkdirs();
    }

//    private static void debugCode() {
//        TreeMap<PrimitiveValue, Integer> treeMap = new TreeMap<PrimitiveValue, Integer>(new PrimValComp());
//        PrimitiveValue primitiveValue1 = new StringValue("vihari");
//        PrimitiveValue primitiveValue2 = new StringValue("harsha");
//        treeMap.put(primitiveValue1, 1);
//        treeMap.put(primitiveValue2, 2);
//        PrimValComp primValComp = new PrimValComp();
//        Set<PrimitiveValue> keySet = treeMap.keySet();
//        for (PrimitiveValue primitiveValue : keySet) {
//            System.out.println(primitiveValue.toRawString());
//            System.out.println(treeMap.get(primitiveValue));
//        }
//    }

//    private static void printSecIndex() {
//        Set<String> tableColNames = Utils.colToSecIndex.keySet();
//        for (String tableColName : tableColNames) {
//            System.out.println(tableColName);
//            TreeMap<PrimitiveValue, List<Integer>> index = Utils.colToSecIndex.get(tableColName);
//            Iterator<PrimitiveValue> iterator = index.keySet().iterator();
//            while (iterator.hasNext()) {
//                PrimitiveValue primitiveValue = iterator.next();
//                List<Integer> pos = index.get(primitiveValue);
//                System.out.println(primitiveValue + " " + Arrays.toString(pos.toArray()));
//            }
//        }
//    }


//    private static void printIndex(PrimaryIndex primaryIndex) {
//        IntegerIndex integerIndex = (IntegerIndex) primaryIndex;
//        System.out.println(Arrays.toString(integerIndex.getPrimaryKeys()));
//        System.out.println(Arrays.toString(integerIndex.getPositions()));
//    }


    public static Operator handleSelect(Select select) {
        TreeBuilder treeBuilder = new TreeBuilder();
        SelectBody selectBody = select.getSelectBody();
        return treeBuilder.handleSelectBody(selectBody, null);
    }

    public static void displayOutput(Operator operator, BufferedWriter bufferedWriter) throws Exception {
        Map<String, Integer> schema = operator.getSchema();
        Map<String, PrimitiveValue> tuple;
        int counter = 1;
       while ((tuple = operator.next()) != null) {
            StringBuilder sb = new StringBuilder();
            Set<String> keySet = tuple.keySet();
            int i = 0;
            for (String key : keySet) {
                sb.append(tuple.get(key).toRawString());
                if (i < keySet.size() - 1)
                    sb.append("|");

                i += 1;
            }
//            bufferedWriter.write(String.valueOf(counter));
//            bufferedWriter.write(". ");
            bufferedWriter.write(sb.toString() + "\n");
            counter++;
        }
//        printOperatorTree(operator, bufferedWriter);
        bufferedWriter.flush();

    }


    public static void printSchema(Map<String, Integer> schema, BufferedWriter bufferedWriter) throws Exception {
        Set<String> colNames = schema.keySet();
        for (String col : colNames) {
            bufferedWriter.write(col + " ");
        }
    }

    public static void printOperatorTree(Operator operator, BufferedWriter bufferedWriter) throws Exception {
        if (operator instanceof ProjectionOperator) {
            ProjectionOperator projectionOperator = (ProjectionOperator) operator;
            projectionOperator.getSchema();
            bufferedWriter.write("ProjectionOperator with schema: ");
            printSchema(projectionOperator.getSchema(), bufferedWriter);
            bufferedWriter.newLine();
            printOperatorTree(projectionOperator.getChild(), bufferedWriter);
        }
        if (operator instanceof SelectionOperator) {
            SelectionOperator selectionOperator = (SelectionOperator) operator;
            bufferedWriter.write("Selection Operator, where condition: " + selectionOperator.getWhereExp().toString());
            bufferedWriter.newLine();
            printOperatorTree(selectionOperator.getChild(), bufferedWriter);
        }
        if (operator instanceof JoinOperator) {
            JoinOperator joinOperator = (JoinOperator) operator;
            if (joinOperator.getJoin().isSimple()) {
                bufferedWriter.write("Simple Join Operator");
            } else if (joinOperator.getJoin().isNatural()) {
                bufferedWriter.write("Natural Join Operator");
            } else {
                bufferedWriter.write("Equi Join on " + joinOperator.getJoin().getOnExpression());
            }
            bufferedWriter.newLine();
            bufferedWriter.write("Join left child");
            bufferedWriter.newLine();
            printOperatorTree(joinOperator.getLeftChild(), bufferedWriter);
            bufferedWriter.write("Join right child");
            bufferedWriter.newLine();
            printOperatorTree(joinOperator.getRightChild(), bufferedWriter);
        }
        if (operator instanceof UnionOperator) {
            UnionOperator unionOperator = (UnionOperator) operator;
            bufferedWriter.write("Union Operator");
            bufferedWriter.newLine();
            bufferedWriter.write("Left Child: ");
            bufferedWriter.newLine();
            printOperatorTree(unionOperator.getLeftChild(), bufferedWriter);
            bufferedWriter.write("Right Child");
            printOperatorTree(unionOperator.getRightChild(), bufferedWriter);
            bufferedWriter.newLine();
        }


        if (operator instanceof TableScan) {
            TableScan tableScan = (TableScan) operator;
            bufferedWriter.write("TableScan Operator on table " + tableScan.getTableName());
            bufferedWriter.newLine();
            if (tableScan.isView()) {
                bufferedWriter.write("View Expression " + Utils.viewToExpression.get(tableScan.getTableName()).toString());
                bufferedWriter.newLine();
            }
        }
        if (operator instanceof ProjectedTableScan) {
            ProjectedTableScan projectedTableScan = (ProjectedTableScan) operator;
            bufferedWriter.write("Projected table scan operator on table " + projectedTableScan.getTableName() + " with schema: ");
            bufferedWriter.newLine();
            printSchema(projectedTableScan.getSchema(), bufferedWriter);
            bufferedWriter.newLine();
            if (projectedTableScan.isView()) {
                bufferedWriter.write("View Expression is " + Utils.viewToExpression.get(projectedTableScan.getTableName()));
            }
            bufferedWriter.newLine();
        }
        if (operator instanceof InMemoryCacheOperator) {
            InMemoryCacheOperator memoryCacheOperator = (InMemoryCacheOperator) operator;
            bufferedWriter.write("InMemoryCacheOperator");
            bufferedWriter.newLine();
            printOperatorTree(memoryCacheOperator.getChild(), bufferedWriter);
        }
        if (operator instanceof OnDiskCacheOperator) {
            OnDiskCacheOperator diskCacheOperator = (OnDiskCacheOperator) operator;
            bufferedWriter.write("OnDiskCacheOperator");
            bufferedWriter.newLine();
            printOperatorTree(diskCacheOperator.getChild(), bufferedWriter);
        }
        if (operator instanceof GroupByOperator) {
            GroupByOperator groupByOperator = (GroupByOperator) operator;
            bufferedWriter.write("group by operator with schema:");
            bufferedWriter.newLine();
            printSchema(groupByOperator.getSchema(), bufferedWriter);
            bufferedWriter.newLine();
            printOperatorTree(((GroupByOperator) operator).getChild(), bufferedWriter);
        }
        if (operator instanceof OrderByOperator) {
            OrderByOperator orderByOperator = (OrderByOperator) operator;
            bufferedWriter.write("orderby operator");
            bufferedWriter.newLine();
            printOperatorTree(orderByOperator.getChild(), bufferedWriter);
        }
        if (operator instanceof LimitOperator) {
            LimitOperator limitOperator = (LimitOperator) operator;
            bufferedWriter.write("limit operator");
            bufferedWriter.newLine();
            printOperatorTree(limitOperator.getChild(), bufferedWriter);
        }

    }

}
