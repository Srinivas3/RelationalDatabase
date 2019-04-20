package dubstep;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import operators.*;
import buildtree.TreeBuilder;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import operators.joins.JoinOperator;
import utils.Utils;

public class Main {
    private static boolean is_testMode = false;
    private static boolean isPhaseOne = false;
    private static String createStatementsDir = "creates";
    private static boolean areDirsCreated = false;

    public static void main(String args[]) throws ParseException, FileNotFoundException {
        try {
            if (is_testMode) {
                FileInputStream fis = new FileInputStream(new File("nba_queries.txt"));
                System.setIn(fis);
            }
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
            List<Long> execution_times = new ArrayList<Long>();
            CCJSqlParser parser = new CCJSqlParser(System.in);
            Statement statement;
            List<String> createStatements = null;
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(System.out));
            while ((statement = parser.Statement()) != null) {
                if (statement instanceof Select) {
                    if (!isPhaseOne) {
                        loadSavedState();
                    }
                    long startTime = System.currentTimeMillis();
                    Operator root = handleSelect((Select) statement);
                    displayOutput(root, bufferedWriter);
                    long endTime = System.currentTimeMillis();
                    execution_times.add(endTime - startTime);
                } else if (statement instanceof CreateTable) {
                    isPhaseOne = true;
                    if (!areDirsCreated) {
                        createDirs();
                        areDirsCreated = false;
                    }
                    saveTableSchema((CreateTable) statement);
                    saveCreateStatement((CreateTable)statement);

                } else {
                    bufferedWriter.write("Invalid Query");
                }
                bufferedWriter.write("$>" + "\n");
                bufferedWriter.flush();
            }
            if (is_testMode) {
                bufferedWriter.write("The execution times are: ");
                for (Long execution_time : execution_times) {
                    bufferedWriter.write(String.valueOf(execution_time / 1000) + " ");
                }
                bufferedWriter.flush();
            }
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void saveCreateStatement(CreateTable createTableStatement) {
        String createStatement = createTableStatement.toString();
        String tableName = createTableStatement.getTable().getName();
        File createStatementFile = new File(createStatementsDir, tableName);
        try {
            FileWriter fr = new FileWriter(createStatementFile);
            fr.write(createStatement + ";");
            fr.flush();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createDirs() {
        new File(createStatementsDir).mkdir();
    }

    private static void loadSavedState() {
        loadSchemas();
    }

    private static void loadSchemas() {
        File dir = new File(createStatementsDir);
        File[] createStatementFiles = dir.listFiles();
        for (File createStatementFile : createStatementFiles) {
            try {
                CCJSqlParser parser = new CCJSqlParser(new FileInputStream(createStatementFile));
                Statement statement = parser.Statement();
                saveTableSchema((CreateTable) statement);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                try {
                    BufferedReader brReader = new BufferedReader(new FileReader(createStatementFile));
                    System.out.println("An exception occurred in line main line 122");
                    String line = null;
                    while ((line = brReader.readLine()) != null){
                        System.out.println(line);
                    }
                } catch (FileNotFoundException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }
        }

    }

    private static void saveTableSchema(CreateTable statement) {
        CreateTable createTable = statement;
        String tableName = createTable.getTable().getName();
        List<ColumnDefinition> colDefs = createTable.getColumnDefinitions();
        Utils.nameToColDefs.put(tableName, colDefs);
    }

    public static Operator handleSelect(Select select) {
        TreeBuilder treeBuilder = new TreeBuilder();
        SelectBody selectBody = select.getSelectBody();
        return treeBuilder.handleSelectBody(selectBody, null);
    }

    public static void displayOutput(Operator operator, BufferedWriter bufferedWriter) throws Exception {
        //printOperatorTree(operator,bufferedWriter);
        Map<String, Integer> schema = operator.getSchema();
        Map<String, PrimitiveValue> tuple;
        int counter = 1;
        long time1 = System.currentTimeMillis();
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
            //bufferedWriter.write(counter);
            //bufferedWriter.write(". ");
            bufferedWriter.write(sb.toString() + "\n");
            counter++;
        }
        bufferedWriter.flush();

    }


    //long time2 = System.currentTimeMillis();
    //bufferedWriter.write(time2-time1);


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


        if (operator instanceof TableScan) {
            TableScan tableScan = (TableScan) operator;
            bufferedWriter.write("TableScan Operator on table " + tableScan.getTableName());
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

    }

}
