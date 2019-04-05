package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.OrderByElement;
import schema.Utils;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

public class OrderByOperator implements Operator {
    static int mergefilecount = 0;
    Operator child;
    List<OrderByElement> orderByElements;
    int counter = -1;
    Map<String, PrimitiveValue> childTuple;
    Map<String, Integer> colNameToIdx;
    Map<Integer, String> idxToColName;
    List<List<PrimitiveValue>> serializedChildTuples;
    Map<String, PrimitiveValue> firstTuple;
    boolean isFirstCall;
    long maxInMemoryTuples;
    List<String> sortedFiles;
    int fileNameCounter;
    long mergeNumber;
    String directoryName;
    String finalSortedFileName;
    PriorityQueue<PriorityQueueTuple> diskSortPriorityQueue;
    CompareTuples compareTuples;
    private Map<String, Integer> schema;
    private ObjectInputStream sortedFileOis;
//    private int insertInPriorityQueueCallCounter = 0;

    public OrderByOperator(List<OrderByElement> orderByElements, Operator operator) {
        this.orderByElements = orderByElements;
        this.child = operator;
        setSchema();
        this.childTuple = child.next();
        colNameToIdx = new LinkedHashMap<String, Integer>();
        idxToColName = new LinkedHashMap<Integer, String>();
        Utils.fillColIdx(childTuple, colNameToIdx, idxToColName);
        isFirstCall = true;
        compareTuples = new CompareTuples();
    }

    public OrderByOperator(List<OrderByElement> orderByElements, Operator operator, Map<String, PrimitiveValue> firstTuple) {
        this.orderByElements = orderByElements;
        this.child = operator;
        setSchema();
        this.childTuple = firstTuple;
        colNameToIdx = new LinkedHashMap<String, Integer>();
        idxToColName = new LinkedHashMap<Integer, String>();
        Utils.fillColIdx(childTuple, colNameToIdx, idxToColName);
        isFirstCall = true;
        compareTuples = new CompareTuples();
    }

    private void setSchema() {
        this.schema = child.getSchema();
    }

    public Map<String, Integer> getSchema() {
        return schema;
    }

    public Map<String, PrimitiveValue> next() {
        if (Utils.inMemoryMode)
            return inMemorySortNext();
        else
            return onDiskSortNext();
    }

    private Map<String, PrimitiveValue> inMemorySortNext() {
        if (isFirstCall) {
            isFirstCall = false;
            maxInMemoryTuples = 1000000;
            serializedChildTuples = new ArrayList<List<PrimitiveValue>>();
            populateChildTuples();
            Collections.sort(serializedChildTuples, compareTuples);
        }
        if (++counter < serializedChildTuples.size()) {
            return Utils.convertToMap(serializedChildTuples.get(counter), idxToColName);
        } else {
            serializedChildTuples.clear();
            serializedChildTuples = null;
            return null;
        }

    }

    private Map<String, PrimitiveValue> readNextTupleFromSortedFile() {
        List<PrimitiveValue> primValTuple = null;
        try {
//            primValTuple = (List<PrimitiveValue>) sortedFileOis.readObject();
            primValTuple = (List<PrimitiveValue>) sortedFileOis.readUnshared();
        } catch (EOFException e) {
            closeInputStream(sortedFileOis);
//            System.out.println("Inside EOF exception in on disk sort return null");
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Utils.convertToMap(primValTuple, idxToColName);
    }

    private Map<String, PrimitiveValue> onDiskSortNext() {
        if (!isFirstCall) {
            return readNextTupleFromSortedFile();
        }
        isFirstCall = false;
        fileNameCounter = 0;
        directoryName = "sorted_files_" + UUID.randomUUID();
        finalSortedFileName = directoryName + "/final_sorted_file" + UUID.randomUUID();
        serializedChildTuples = new ArrayList<List<PrimitiveValue>>();
        new File(directoryName).mkdir();
        maxInMemoryTuples = 10000;
        while (childTuple != null) {
            populateChildTuples();
            String fileName = getNewFileName();
            sortAndWriteToFile(fileName);
        }
        child = null;
        serializedChildTuples = null;
//        System.gc();
        initPriorityQueue();
        mergeAllFiles();
        sortedFileOis = openObjectInputStream(finalSortedFileName);
        return readNextTupleFromSortedFile();
    }

    private void mergeAllFiles() {
        mergefilecount++;
        try {
            List<PrimitiveValue> nextTuplePrimVal = getNextValueAndUpdateQueue();
            ObjectOutputStream sortedFileOos = openObjectOutputStream(finalSortedFileName);
            while (nextTuplePrimVal != null) {

                sortedFileOos.writeUnshared(nextTuplePrimVal);
                sortedFileOos.reset();
                nextTuplePrimVal = getNextValueAndUpdateQueue();
            }
            sortedFileOos.flush();
            sortedFileOos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private String getNewFileName() {
        fileNameCounter++;
        return getFileName(fileNameCounter);
    }

    private String getFileName(int fileNameCounter) {
        return String.valueOf(directoryName) + "/" + String.valueOf(fileNameCounter);
    }

    private void sortAndWriteToFile(String fileName) {
        Collections.sort(serializedChildTuples, compareTuples);
        FileOutputStream fos = null;
        try {
            ObjectOutputStream oos = openObjectOutputStream(fileName);
            for (List<PrimitiveValue> serializedChildTuple : serializedChildTuples) {
                oos.writeUnshared(serializedChildTuple);
            }
            oos.flush();
            oos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        serializedChildTuples.clear();
    }

    private ObjectOutputStream openObjectOutputStream(String fileName) throws IOException {
        FileOutputStream fos;
        File file = new File(fileName);
        fos = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos);
        return new ObjectOutputStream(bufferedOutputStream);
    }

    public void initPriorityQueue() {
        int i = 1;
        diskSortPriorityQueue = new PriorityQueue<PriorityQueueTuple>();
        while (i <= fileNameCounter) {
            String fileName = getFileName(i);
            ObjectInputStream ois = openObjectInputStream(fileName);
            insertInPriorityQueue(ois);
            i++;
        }

    }

    private ObjectInputStream openObjectInputStream(String fileName) {
        ObjectInputStream ois = null;
        try {
            FileInputStream fis = new FileInputStream(fileName);
            BufferedInputStream bis = new BufferedInputStream(fis);
            ois = new ObjectInputStream(bis);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ois;
    }

    private void insertInPriorityQueue(ObjectInputStream ois) {
//        insertInPriorityQueueCallCounter = 0;
        List<PrimitiveValue> tuple = null;
        try {
//            tuple = (List<PrimitiveValue>) ois.readObject();
            tuple = (List<PrimitiveValue>) ois.readUnshared();
//            if (insertInPriorityQueueCallCounter % 1000 == 0){
//                int x = 0;
//            }
//            insertInPriorityQueueCallCounter++;
        } catch (EOFException e) {
            closeInputStream(ois);
            return;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (tuple == null) {
            closeInputStream(ois);
            return;
        }
        PriorityQueueTuple priorityQueueTuple = new PriorityQueueTuple(tuple, ois);
        diskSortPriorityQueue.add(priorityQueueTuple);
    }

    private void closeInputStream(ObjectInputStream ois) {
        try {
            ois.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return;
    }

    private List<PrimitiveValue> getNextValueAndUpdateQueue() {
        PriorityQueueTuple priorityQueueTuple = diskSortPriorityQueue.poll();
        if (priorityQueueTuple == null)
            return null;
        List<PrimitiveValue> tuple = priorityQueueTuple.getTuple();
        ObjectInputStream tupleOis = priorityQueueTuple.getTupleOis();
        insertInPriorityQueue(tupleOis);
        return tuple;
    }


    public void init() {

    }


    private void populateChildTuples() {
        long counter = 0;
//        serializedChildTuples = new ArrayList<List<PrimitiveValue>>();
        if (firstTuple != null) {
            serializedChildTuples.add(Utils.convertToList(firstTuple, colNameToIdx));
            firstTuple = null;
        }
        while (childTuple != null && counter < maxInMemoryTuples) {
            List<PrimitiveValue> serializedChildTuple = Utils.convertToList(childTuple, colNameToIdx);
            serializedChildTuples.add(serializedChildTuple);
            childTuple = child.next();
            counter++;
        }

    }


    class CompareTuples extends Eval implements Comparator<List<PrimitiveValue>> {

        Map<String, PrimitiveValue> currTuple;

        public int compare(List<PrimitiveValue> o1, List<PrimitiveValue> o2) {
            return compareMaps(Utils.convertToMap(o1, idxToColName), Utils.convertToMap(o2, idxToColName));
        }

        public int compareMaps(Map<String, PrimitiveValue> o1, Map<String, PrimitiveValue> o2) {

//            if (o1 == null || o2 == null){
//                System.out.println(o1);
//                System.out.println(o2);
//            }

//            if(o1 == null){
//                return -1;
//            }
//            if (o2 == null){
//                return 1;
//            }


            for (OrderByElement element : orderByElements) {
                Expression expression = element.getExpression();
                boolean isAsc = element.isAsc();

                PrimitiveValue value1 = evaluate(expression, o1);
                PrimitiveValue value2 = evaluate(expression, o2);
                if (value1.getType().equals(PrimitiveType.DOUBLE)) {
                    try {
                        double val1 = value1.toDouble();
                        double val2 = value2.toDouble();
                        if (val1 < val2) {
                            return isAsc ? -1 : 1;
                        } else if (val1 > val2) {
                            return isAsc ? 1 : -1;
                        }
                    } catch (PrimitiveValue.InvalidPrimitive throwables) {
                        throwables.printStackTrace();
                    }
                } else if (value1.getType().equals(PrimitiveType.LONG)) {
                    try {
                        long val1 = value1.toLong();
                        long val2 = value2.toLong();
                        if (val1 < val2) {
                            return isAsc ? -1 : 1;
                        } else if (val1 > val2) {
                            return isAsc ? 1 : -1;
                        }
                    } catch (PrimitiveValue.InvalidPrimitive throwables) {
                        throwables.printStackTrace();
                    }
                } else if (value1.getType().equals(PrimitiveType.STRING)) {
                    String val1 = value1.toRawString();
                    String val2 = value2.toRawString();
                    int compare = val1.compareTo(val2);
                    if (compare == 0) continue;
                    if (isAsc) return compare;
                    else return (-1 * compare);

                }

            }
            return 0;
        }

        public PrimitiveValue evaluate(Expression expression, Map<String, PrimitiveValue> tuple) {
            currTuple = tuple;
            PrimitiveValue value = null;
            try {
                value = eval(expression);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return value;
        }

        public PrimitiveValue eval(Column x) {

            String colName = x.getColumnName();
            String tableName = x.getTable().getName();
            return Utils.getColValue(tableName, colName, currTuple);
        }
    }

    class PriorityQueueTuple implements Comparable<PriorityQueueTuple> {
        private List<PrimitiveValue> tuple;
        private ObjectInputStream tupleOis;

        public PriorityQueueTuple(List<PrimitiveValue> tuple, ObjectInputStream tupleOis) {
            this.tuple = tuple;
            this.tupleOis = tupleOis;
        }


        public int compareTo(PriorityQueueTuple o) {
            return compareTuples.compare(this.tuple, o.tuple);
        }

        public List<PrimitiveValue> getTuple() {
            return tuple;
        }

        public ObjectInputStream getTupleOis() {
            return tupleOis;
        }
    }


}


