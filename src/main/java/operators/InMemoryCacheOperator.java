package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;
import schema.Utils;

import java.util.*;

public class InMemoryCacheOperator implements Operator {
    Operator child;
    Map<String,PrimitiveValue> childTuple;
    Map<String,Integer> colNameToIdx;
    Map<Integer,String> idxToColName;
    List<List<PrimitiveValue>> cacheMemory;
    boolean isCached;
    Map<String,PrimitiveValue> returnTuple;
    Iterator<List<PrimitiveValue>> cacheIterator;
    public InMemoryCacheOperator(Operator child){
        colNameToIdx = new LinkedHashMap<String,Integer>();
        idxToColName = new LinkedHashMap<Integer,String>();
        returnTuple = new LinkedHashMap<String, PrimitiveValue>();
        cacheMemory = new ArrayList<List<PrimitiveValue>>();
        this.child = child;
        this.childTuple = child.next();
        Utils.fillColIdx(childTuple,colNameToIdx,idxToColName);
        isCached = false;
    }
    public Map<String, PrimitiveValue> next() {
        if (childTuple == null){
            isCached = true;
            return null;
        }
        if (isCached){
            returnTuple =  childTuple;
            if (cacheIterator.hasNext()){
                childTuple = Utils.convertToMap(cacheIterator.next(),idxToColName);
            }
            else{
                childTuple = null;
            }
            return returnTuple;
        }
        else{
            cacheMemory.add(Utils.convertToList(childTuple,colNameToIdx));
            returnTuple.putAll(childTuple);
            childTuple = child.next();
            return returnTuple;
        }

    }

    public void init() {
        cacheIterator = cacheMemory.iterator();
        childTuple = Utils.convertToMap(cacheIterator.next(),idxToColName);
    }
}
