package mydb.storage;

import java.io.*;
import mydb.execution.Predicate;
import mydb.common.Type;

/**
 * 域（Field）的interface定义
 */
public interface Field extends Serializable {

    /**
     * 表示将该字段（field）的字节写入指定的DataOutputStream
     * @param dos DataOutPutStream
     */
    void serialize(DataOutputStream dos) throws IOException;

    boolean compare(Predicate.Op op, Field value);

    Type getType();

    @Override
    int hashCode();
    @Override
    boolean equals(Object field);
    @Override
    String toString();
}
