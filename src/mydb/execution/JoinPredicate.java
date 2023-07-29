package mydb.execution;

import mydb.storage.Field;
import mydb.storage.Tuple;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;


/**
 * JOIN谓词，利用谓词对两个元组进行比较。主要用于JOIN操作符
 */
public class JoinPredicate implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private final int fieldIndex1;
    private final Predicate.Op op;
    private final int fieldIndex2;

    /**
     * JoinPredicate构造函数，创建一个谓词实例用于比较两个元组的两个字段
     * @param fieldIndex1 字段索引
     * @param op 谓词比较符（GREATER_THAN、LESS_THAN、EQUAL等）
     * @param fieldIndex2 字段索引
     */
    public JoinPredicate(int fieldIndex1, Predicate.Op op, int fieldIndex2) {
        this.fieldIndex1 = fieldIndex1;
        this.op = op;
        this.fieldIndex2 = fieldIndex2;
    }

    /**
     * @return 若操作的两个元组均满足谓词条件则返回true，反之返回false
     */
    public boolean filter(Tuple tuple1, Tuple tuple2) {
        if (tuple1 == null || tuple2 == null) {
            return false;
        }
        Field firstField = tuple1.getField(fieldIndex1);
        Field secondField = tuple2.getField(fieldIndex2);
        return firstField.compare(op, secondField);
    }

    public int getFieldIndex1() {
        return fieldIndex1;
    }

    public int getFieldIndex2() {
        return fieldIndex2;
    }

    public Predicate.Op getOp() {
        return op;
    }
}
