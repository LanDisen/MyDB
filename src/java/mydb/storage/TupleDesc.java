package java.mydb.storage;

import java.mydb.common.Type;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc（tuple descriptor）类用于描述一个元组（tuple）的模式（schema）
 * TupleDesc包括Type对象的集合，每个TupleDesc对象都描述了相应字段（Field）的类型（Type）
 */
public class TupleDesc {

    // 内部类，用于组织每个域的信息
    public static class TupleDescItem implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        public final Type fieldType; // 字段（Field）的类型（Type）
        public final String fieldName; // 字段（field）的名称

        public TupleDescItem(Type type, String name) {
            this.fieldName = name;
            this.fieldType = type;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldName + ")";
        }

    }

    /**
     * @return 返回该TupleDesc对象所有字段TupleDescItems的迭代器
     */
    public Iterator<TupleDescItem> iterator() {
        // TODO
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * TupleDesc构造函数
     * @param fieldTypes 字段（field）类型数组
     * @param fieldNames 字段（field）名称数组，name可以为null
     */
    public TupleDesc(Type[] fieldTypes, String[] fieldNames) {
        // TODO
    }

    /**
     * TupleDesc构造函数
     * @param fieldTypes 字段（field）类型数组
     */
    public TupleDesc(Type[] fieldTypes) {
        // TODO
    }

    /**
     * @return 返回字段（fields）的数量
     */
    public int getFieldsNum() {
        // TODO
        return 0;
    }

    /**
     * 获取第index个字段名称（可能为null）
     * @param index 字段名称的索引值
     * @return 第index个字段的名称
     * @throws NoSuchElementException 如果index不是一个有效的field索引
     */
    public String getFieldName(int index) throws NoSuchElementException {
        // TODO
        return null;
    }

    /**
     * 获取该TupleDesc对象的第index个字段的类型
     * @param index 字段类型的索引值
     * @return 第index个字段的类型
     * @throws NoSuchElementException 如果index不是一个有效的field索引
     */
    public Type getFieldType(int index) throws NoSuchElementException {
        // TODO
        return null;
    }

    /**
     * 根据字段名称获取对应的索引值
     * @param fieldName 字段名称
     * @return 字段名称对应的索引值
     * @throws NoSuchElementException 没有匹配的字段名称
     */
    public int fieldNameToIndex(String fieldName) throws NoSuchElementException {
        // TODO
        return 0;
    }

    /**
     * @return 返回元组（Tuple）的字节大小
     */
    public int getSize() {
        // TODO
        return 0;
    }

    /**
     * 合并两个TupleDesc对象，新对象的字段数（fieldNum）为：td1.getFieldsNum() + td2.getFieldsNum()
     * @param td1 TupleDesc对象
     * @param td2 TupleDesc对象
     * @return 新TupleDesc对象
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // TODO
        return null;
    }

    /**
     * 用于与该TupleDesc进行比较
     * 两个TupleDesc如果拥有相同的字段数量，并且每个字段的类型（Type）均相同，则认为相同
     * @param obj 用于与该TupleDesc进行比较的Object对象
     * @return 相同则返回true，反之为false
     */
    @Override
    public boolean equals(Object obj) {
        // TODO
        return false;
    }

    @Override
    public int hashCode() {
        // 如果需要使用TupleDesc作为HashMap的keys则实现该方法使得相同对象有相同的hashCode
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * 格式："fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])"
     * @return 描述该TupleDesc的字符串
     */
    @Override
    public String toString() {
        // TODO
        return "";
    }
}
