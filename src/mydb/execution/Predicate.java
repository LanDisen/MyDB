package mydb.execution;

import java.io.Serial;
import mydb.storage.Field;
import mydb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate（谓词）类用于比较tuple和具体的字段（field）
 * 谓词的返回值都是true，如：NOT NULL、EXISTS、IN等都是谓词
 */
public class Predicate implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 枚举类，用于返回Field.compare常数值
     */
    public enum Op implements Serializable {
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQ,
        LESS_THAN,
        LESS_THAN_OR_EQ,
        LIKE;

        /**
         * @param i Op索引下标
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == NOT_EQUALS)
                return "<>";
            if (this == GREATER_THAN)
                return ">";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == LIKE)
                return "LIKE";
            throw new IllegalStateException("illegal operator");
        }
    }

    private final int fieldIndex;

    private final Op op;

    /**
     * 操作数
     */
    private final Field operand;

    /**
     * Predicate谓词构造函数
     * @param fieldIndex 要进行比较的字段索引号（field number）
     * @param op 比较操作符
     * @param operand 具体字段（field）的值
     */
    public Predicate(int fieldIndex, Op op, Field operand) {
        this.fieldIndex = fieldIndex;
        this.op = op;
        this.operand = operand;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    /**
     * @return 返回操作符
     */
    public Op getOp() {
        return op;
    }

    /**
     * @return 返回操作数
     */
    public Field getOperand() {
        return operand;
    }

    /**
     * 将构造函数中指定的字段号和指定的操作数进行比较
     * @param tuple 要进行比较的元组tuple
     * @return 如果比较结果是true则返回true，否则返回false
     */
    public boolean filter(Tuple tuple) {
        if (tuple == null) {
            return false;
        }
        Field field = tuple.getField(this.fieldIndex);
        return field.compare(this.op, this.operand);
    }

    @Override
    public String toString() {
        return "fieldIndex:" + fieldIndex + " op:" + op.toString() + " operand:" + operand.toString();
    }
}
