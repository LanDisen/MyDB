package mydb.storage;

import mydb.execution.Predicate;
import mydb.common.Type;

import java.io.*;

/**
 * 存储单个integer的字段的实例
 */
public class IntField implements Field {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int value;

    public int getValue() {
        return value;
    }

    public Type getType() {
        return Type.INT_TYPE;
    }

    /**
     * @param value 该字段存放的具体的值（属性）
     */
    public IntField(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Integer.toString((value));
    }

    @Override
    public int hashCode() {
        return value;
    }

    public boolean equals(Object field) {
        if (!(field instanceof IntField)) return false;
        return ((IntField) field).value == value;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }

    @Override
    public boolean compare(Predicate.Op op, Field value) {
        IntField intField = (IntField) value;
        return switch (op) {
            case EQUALS, LIKE -> this.value == intField.value;
            case NOT_EQUALS -> this.value != intField.value;
            case GREATER_THAN -> this.value > intField.value;
            case GREATER_THAN_OR_EQ -> this.value >= intField.value;
            case LESS_THAN -> this.value < intField.value;
            case LESS_THAN_OR_EQ -> this.value <= intField.value;
        };
    }

}
