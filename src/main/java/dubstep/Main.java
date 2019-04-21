package dubstep;

import java.io.*;
import java.util.*;

import Indexes.IndexFactory;
import Indexes.IntegerIndex;
import Indexes.PrimaryIndex;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.Index;
import operators.*;
import buildtree.TreeBuilder;
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
    private static String colDefsDir = "colDefsDir";
    private static boolean areDirsCreated = false;
    public static String compressedTablesDir = "compressedTablesDir";
    public static String linesDir = "lines";
    public static String primaryIndexDir = "primaryIndicesDir";
    private static String format = ".csv";
    private static boolean isFirstSelect = true;
    private static IndexFactory indexFactory = new IndexFactory();

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
                    if (!isPhaseOne && isFirstSelect) {
                        loadSavedState();
                        isFirstSelect = false;
                    }
                    long startTime = System.currentTimeMillis();
                    Operator root = handleSelect((Select) statement);
                    displayOutput(root, bufferedWriter);
                    long endTime = System.currentTimeMillis();
                    execution_times.add(endTime - startTime);
                } else if (statement instanceof CreateTable) {
                    CreateTable createTableStatement = (CreateTable) statement;
                    isPhaseOne = true;
                    if (!areDirsCreated) {
                        createDirs();
                        areDirsCreated = false;
                    }
                    saveTableSchema(createTableStatement);
                    saveColDefsToDisk(createTableStatement);
                    scanTable(createTableStatement);
                    saveTableToLines(createTableStatement);
                    buildIndexAndSave(createTableStatement);
//                    printIndex(Utils.colToIndexes.get("R.A"));
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

    private static void buildIndexAndSave(CreateTable createTableStatement) {
        Table table = createTableStatement.getTable();
        List<Index> indexes = createTableStatement.getIndexes();
        if (indexes == null || indexes.isEmpty()) {
            return;
        }
        for (Index index : indexes) {
            if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                String colName = index.getColumnsNames().get(0);
                PrimaryIndex primaryIndex = indexFactory.getIndex(table, colName);
                String keyFileName = table.getName() + "." + colName;
                Utils.colToIndexes.put(keyFileName, primaryIndex);
                writeIndexToFile(keyFileName, primaryIndex);
            }
        }
    }

    private static void writeIndexToFile(String keyFileName, PrimaryIndex primaryIndex) {
        File indexFile = new File(primaryIndexDir, keyFileName);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(indexFile);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
            primaryIndex.serializeToStream(dataOutputStream);
            dataOutputStream.flush();
            dataOutputStream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void saveTableToLines(CreateTable createTableStatement) {
        String tableName = createTableStatement.getTable().getName();
        File file = new File(linesDir, tableName);
        try {
            DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file));
            dataOutputStream.writeInt(Utils.tableToLines.get(tableName));
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void scanTable(CreateTable createTableStatement) {
        Table table = createTableStatement.getTable();
        String tableName = table.getName();
        File tableFile = new File("data", tableName + format);
        File compressedTableFile = new File(compressedTablesDir, tableName);
        List<ColumnDefinition> colDefs = Utils.nameToColDefs.get(tableName);
        int numOfLines = 0;

        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(compressedTableFile));
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(tableFile));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String tupleArr[] = line.split("\\|");
                for (int i = 0; i < tupleArr.length; i++) {
                    writeBytes(dataOutputStream, tupleArr[i], colDefs.get(i));
                }
                numOfLines++;

            }
            Utils.tableToLines.put(tableName, numOfLines);
            bufferedReader.close();
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeBytes(DataOutputStream dataOutputStream, String colInString, ColumnDefinition columnDefinition) {
        String dataType = columnDefinition.getColDataType().getDataType().toLowerCase();
        try {
            if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("date")) {
                dataOutputStream.writeInt(colInString.length());
                dataOutputStream.write(colInString.getBytes());
            } else if (dataType.equalsIgnoreCase("int")) {
                dataOutputStream.writeInt(Integer.valueOf(colInString));
            } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
                dataOutputStream.writeDouble(Double.valueOf(colInString));
            } else {
                throw new Exception("invalid data type " + dataType);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void saveColDefsToDisk(CreateTable createTableStatement) {
        List<ColumnDefinition> columnDefinitions = createTableStatement.getColumnDefinitions();
        String tableName = createTableStatement.getTable().getName();
        File colDefsFile = new File(colDefsDir, tableName);
        try {
            FileWriter fileWriter = new FileWriter(colDefsFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (ColumnDefinition colDef : columnDefinitions) {
                String colName = colDef.getColumnName();
                String colDataType = colDef.getColDataType().getDataType();
                bufferedWriter.write(colName + "," + colDataType);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createDirs() {
        new File(colDefsDir).mkdir();
        new File(compressedTablesDir).mkdir();
        new File(linesDir).mkdir();
        new File(primaryIndexDir).mkdir();
    }

    private static void loadSavedState() {
        loadSchemas();
        loadTableLines();
        loadIndexes();
    }

    private static void loadIndexes() {
        File primaryIndexesDirFile = new File(primaryIndexDir);
        File[] files = primaryIndexesDirFile.listFiles();
        for (File indexFile : files) {
            loadIndex(indexFile);
        }
    }

    private static void loadIndex(File indexFile) {
        PrimaryIndex primaryIndex = indexFactory.getIndex(indexFile);
        Utils.colToIndexes.put(indexFile.getName(), primaryIndex);
        //printIndex(primaryIndex);
    }

    private static void printIndex(PrimaryIndex primaryIndex) {
        IntegerIndex integerIndex = (IntegerIndex) primaryIndex;
        System.out.println(Arrays.toString(integerIndex.getPrimaryKeys()));
        System.out.println(Arrays.toString(integerIndex.getPositions()));
    }

    private static void loadTableLines() {
        File dir = new File(linesDir);
        File[] files = dir.listFiles();
        for (File file : files) {
            try {
                DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file));
                Utils.tableToLines.put(file.getName(), dataInputStream.readInt());
                dataInputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void loadSchemas() {
        File dir = new File(colDefsDir);
        File[] colDefFiles = dir.listFiles();
        for (File colDefsFile : colDefFiles) {
            saveTableSchema(colDefsFile);
        }

    }

    private static void saveTableSchema(File colDefsFile) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(colDefsFile));
            List<ColumnDefinition> columnDefinitions = new ArrayList<ColumnDefinition>();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String parts[] = line.split(",");
                ColumnDefinition columnDefinition = new ColumnDefinition();
                columnDefinition.setColumnName(parts[0]);
                ColDataType colDataType = new ColDataType();
                colDataType.setDataType(parts[1]);
                columnDefinition.setColDataType(colDataType);
                columnDefinitions.add(columnDefinition);
            }
            Utils.nameToColDefs.put(colDefsFile.getName(), columnDefinitions);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
