package java.mydb.storage;

import java.mydb.common.Type;
import java.mydb.execution.Predicate;

import java.io.*;

/**
 * 存储单个固定长度的String的字段实例
 */
public class StringField implements Field {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String value;

    private final int maxSize;

    public String getValue() {
        return value;
    }

    public Type getType() {
        return Type.STRING_TYPE;
    }

    /**
     * StringField构造函数
     * @param s StringField字符串字段的值
     * @param maxSize 字符串值的最大长度
     */
    public StringField(String s, int maxSize) {
        this.maxSize = maxSize;
        if (s.length() > maxSize) {
            value = s.substring(0, maxSize);
        } else {
            value = s;
        }
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /**
     * 将字符串写到dos中。总会写maxsize+4个字节到dos中。
     * 前4个字节是字符串长度，后面字节为字符串内容
     * @param dos DataOutPutStream
     */
    public void serialize(DataOutputStream dos) throws IOException {
        String s = value;
        int overflow = maxSize - s.length();
        if (overflow < 0) {
            s = s.substring(0, maxSize);
        }
        dos.writeInt(s.length());
        dos.writeBytes(s);
        while (overflow-- > 0)
            dos.write((byte) 0);
    }

    /**
     * 用于比较指定字段和该字段的值
     */
    public boolean compare(Predicate.Op op, Field value) {
        StringField stringField = (StringField) value;
        int cmpValue = this.value.compareTo(stringField.value);
        return switch (op) {
            case EQUALS -> cmpValue == 0;
            case NOT_EQUALS -> cmpValue != 0;
            case GREATER_THAN -> cmpValue > 0;
            case GREATER_THAN_OR_EQ -> cmpValue >= 0;
            case LESS_THAN -> cmpValue < 0;
            case LESS_THAN_OR_EQ -> cmpValue <= 0;
            case LIKE -> this.value.contains(stringField.value);
        };
    }

}
