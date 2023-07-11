package java.mydb.storage;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple类用于维护元组（tuple）信息
 * Tuple由字段（Field）集合组成
 */
public class Tuple implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Tuple构造函数
     * @param td 该元组（tuple）的模式（schema）。TupleDesc应至少有一个字段（field）
     */
    public Tuple(TupleDesc td) {
        // TODO
    }

    /**
     * @return 返回该元组（tuple）的TupleDesc模式（schema）
     */
    public TupleDesc getTupleDesc() {
        // TODO
        return null;
    }

    /**
     * @return 返回该元组在磁盘中的对应RecordId（可能为null）
     */
    public RecordId getRecordId() {
        // TODO
        return null;
    }

    /**
     * 设置该元组的RecordId
     * @param recordId 该元组（tuple）的新RecordId
     */
    public void setRecordId(RecordId recordId) {
        // TODO
    }

    /**
     * @param index 字段索引
     * @return 返回第index个字段（field）的值
     */
    public Field getField(int index) {
        // TODO
        return null;
    }

    /**
     * 设置第index个字段的值
     * @param index 需要设置的字段的索引
     * @param field 字段（field）的新值
     */
    public void setField(int index, Field field) {
        // TODO
    }

    /**
     * 格式："column1\tcolumn2\t...\tcolumnN"
     * @return 返回该Tuple的字符串信息
     */
    @Override
    public String toString() {
        // TODO
        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return 该元组的字段集合的迭代器（Iterator）
     */
    public Iterator<Field> fields() {
        // TODO
        return null;
    }

    /**
     * 重新设置该元组的TupleDesc
     */
    public void resetTupleDesc(TupleDesc td) {
        // TODO
    }

}
