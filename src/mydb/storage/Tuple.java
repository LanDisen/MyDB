package mydb.storage;

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

    private TupleDesc tupleDesc;
    private Field[] fields;
    private RecordId recordId;

    /**
     * Tuple构造函数
     * @param td 该元组（tuple）的模式（schema）。TupleDesc应至少有一个字段（field）
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        this.fields = new Field[td.getFieldsNum()];
    }

    /**
     * @return 返回该元组（tuple）的TupleDesc模式（schema）
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return 返回该元组在磁盘中的对应RecordId（可能为null）
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * 设置该元组的RecordId
     * @param recordId 该元组（tuple）的新RecordId
     */
    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }

    /**
     * @param index 字段索引
     * @return 返回第index个字段（field）的值
     */
    public Field getField(int index) {
        return fields[index];
    }

    /**
     * 设置第index个字段的值
     * @param index 需要设置的字段的索引
     * @param field 字段（field）的新值
     */
    public void setField(int index, Field field) {
        this.fields[index] = field;
    }

    /**
     * 格式："column1\tcolumn2\t...\tcolumnN"
     * @return 返回该Tuple的字符串信息
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        int n = fields.length;
        for (int i=0; i<n; i++) {
            str.append(fields[i]);
            if (i < n - 1)
                str.append('\t');
        }
        return str.toString();
    }

    /**
     * @return 该元组的字段集合的迭代器（Iterator）
     */
    public Iterator<Field> fields() {
        return Arrays.asList(this.fields).iterator();
    }

    /**
     * 重新设置该元组的TupleDesc
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
        //this.fields = new Field[td.getFieldsNum()];
    }

}
