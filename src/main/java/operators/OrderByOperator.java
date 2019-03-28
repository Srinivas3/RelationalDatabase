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
    Operator child;
    List<OrderByElement> orderByElements;
    int counter = -1;
    Map<String, PrimitiveValue> childTuple;
    Map<String, Integer> colNameToIdx;
    Map<Integer, String> idxToColName;
    List<List<PrimitiveValue>> serializedChildTuples;
    boolean isFirstCall;
    long maxInMemoryTuples;
    List<String> sortedFiles;
    int fileNameCounter;
    long mergeNumber;
    String directoryName;
    PriorityQueue<PriorityQueueTuple> diskSortPriorityQueue;
    CompareTuples compareTuples;

    public OrderByOperator(List<OrderByElement> orderByElements, Operator operator) {
        this.orderByElements = orderByElements;
        this.child = operator;
        this.childTuple = child.next();
        colNameToIdx = new LinkedHashMap<String, Integer>();
        idxToColName = new LinkedHashMap<Integer, String>();
        Utils.fillColIdx(childTuple, colNameToIdx, idxToColName);
        isFirstCall = true;
        compareTuples = new CompareTuples();
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
        if (++counter < serializedChildTuples.size())
            return Utils.deserialize(serializedChildTuples.get(counter), idxToColName);
        else
            return null;
    }

    private Map<String, PrimitiveValue> onDiskSortNext() {
        if (!isFirstCall)
            return Utils.deserialize(getNextValueAndUpdateQueue(), idxToColName);
        isFirstCall = false;
        fileNameCounter = 0;
        directoryName = "sorted_files";
        new File(directoryName).mkdir();
        maxInMemoryTuples = 10000;
        while (childTuple != null) {
            populateChildTuples();
            String fileName = getNewFileName();
            sortAndWriteToFile(fileName);
        }
        serializedChildTuples = null;
        initPriorityQueue();
        return Utils.deserialize(getNextValueAndUpdateQueue(), idxToColName);
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
            File file = new File(fileName);
            fos = new FileOutputStream(file);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            for (List<PrimitiveValue> serializedChildTuple : serializedChildTuples)
                oos.writeObject(serializedChildTuple);
            oos.flush();
            oos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initPriorityQueue() {
        int i = 1;
        diskSortPriorityQueue = new PriorityQueue<PriorityQueueTuple>();
        while (i <= fileNameCounter) {
            String fileName = getFileName(i);
            try {
                FileInputStream fis = new FileInputStream(fileName);
                BufferedInputStream bis = new BufferedInputStream(fis);
                ObjectInputStream ois = new ObjectInputStream(bis);
                insertInPriorityQueue(ois);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            i++;
        }

    }
    private void insertInPriorityQueue(ObjectInputStream ois) {
        List<PrimitiveValue> tuple = null;
        try {
            tuple = (List<PrimitiveValue>) ois.readObject();
        } catch (EOFException e) {
            return;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (tuple == null) {
            try {
                ois.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }
        PriorityQueueTuple priorityQueueTuple = new PriorityQueueTuple(tuple, ois);
        diskSortPriorityQueue.add(priorityQueueTuple);
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
        serializedChildTuples = new ArrayList<List<PrimitiveValue>>();
        while (childTuple != null && counter < maxInMemoryTuples) {
            List<PrimitiveValue> serializedChildTuple = Utils.serialize(childTuple, colNameToIdx);
            serializedChildTuples.add(serializedChildTuple);
            childTuple = child.next();
            counter++;
        }

    }


    class CompareTuples extends Eval implements Comparator<List<PrimitiveValue>> {

        Map<String, PrimitiveValue> currTuple;

        public int compare(List<PrimitiveValue> o1, List<PrimitiveValue> o2) {
            for (OrderByElement element : orderByElements) {
                Expression expression = element.getExpression();
                boolean isAsc = element.isAsc();
                PrimitiveValue value1 = evaluate(expression, Utils.deserialize(o1, idxToColName));
                PrimitiveValue value2 = evaluate(expression, Utils.deserialize(o2, idxToColName));
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


