package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;
import utils.Utils;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OnDiskCacheOperator implements Operator {


    Operator child;
    Map<String, PrimitiveValue> childTuple;
    Map<String, Integer> colNameToIdx;
    Map<Integer, String> idxToColName;
    Map<String, PrimitiveValue> returnTuple;
    Map<String, Integer> schema;
    String cacheFileName;
    boolean isCached;
    boolean isFirstCall;
    boolean isFirstTime;

    ObjectOutputStream objectOutputStream;
    ObjectInputStream objectInputStream;

    public OnDiskCacheOperator(Operator child) {
        colNameToIdx = new LinkedHashMap<String, Integer>();
        idxToColName = new LinkedHashMap<Integer, String>();
        this.child = child;
        this.schema = child.getSchema();
        this.returnTuple = new LinkedHashMap<String, PrimitiveValue>();
        cacheFileName = "cache" + Utils.diskCacheCnt;
        Utils.diskCacheCnt++;
        isCached = false;
        isFirstCall = true;
        isFirstTime = true;
    }

    public Map<String, Integer> getSchema() {
        return schema;
    }

    public Operator getChild() {
        return child;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    public Map<String, PrimitiveValue> next() {
        if (isFirstCall) {
            this.childTuple = child.next();
            Utils.fillColIdx(childTuple, colNameToIdx, idxToColName);
            isFirstCall = false;
        }
        try {
            if (childTuple == null) {
                if (objectInputStream != null) {
                    objectInputStream.close();
                }
                if (objectOutputStream != null) {
                    objectOutputStream.close();
                }
                return null;
            }
            if (isCached) {
                returnTuple.putAll(childTuple);
                childTuple = Utils.convertToMap((List<PrimitiveValue>) objectInputStream.readObject(), idxToColName);
                return returnTuple;
            }
            if (isFirstTime) {
                FileOutputStream fos = new FileOutputStream(cacheFileName);
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                objectOutputStream = new ObjectOutputStream(bos);
                isFirstTime = false;
            }
            objectOutputStream.writeObject(Utils.convertToList(childTuple, colNameToIdx));
            returnTuple.putAll(childTuple);
            childTuple = child.next();
            return returnTuple;

        } catch (EOFException e) {
            childTuple = null;
            return returnTuple;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return returnTuple;
    }

    public void init() {
        isCached = true;
        try {
            FileInputStream fis = new FileInputStream(cacheFileName);
            BufferedInputStream bis = new BufferedInputStream(fis);
            objectInputStream = new ObjectInputStream(bis);
            childTuple = Utils.convertToMap((List<PrimitiveValue>) objectInputStream.readObject(), idxToColName);
            ;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
