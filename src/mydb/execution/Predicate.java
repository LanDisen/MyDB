package mydb.execution;

import java.io.Serial;
import mydb.storage.Field;
import mydb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate（谓词）类用于比较tuple和具体的字段（field）
 */
public class Predicate implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

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

    /**
     * Predicate构造函数
     * @param field 要进行比较的字段号（field number）
     * @param op 比较操作符
     * @param operand 具体字段（field）的值
     */
    public Predicate(int field, Op op, Field operand) {
        // code
    }

    public int getField() {
        // code
        return -1;
    }

    public Op getOp() {
        // code
        return null;
    }

    public Field getOperand() {
        // code
        return null;
    }

    /**
     * 将构造函数中指定的字段号和指定的操作数进行比较
     * @param t 要进行比较的元组tuple
     * @return 如果比较是true则返回true，否则返回false
     */
    public boolean filter(Tuple t) {
        // code
        return false;
    }

    @Override
    public String toString() {
        // code
        return "";
    }
}
