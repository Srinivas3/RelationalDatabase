package Indexes;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.List;

public interface PrimaryIndex {
    long getFilePosition(PrimitiveValue primitiveValue);
}
