package mydb.storage;

import mydb.common.Type;

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
            return fieldName + "(" + fieldType + ")";
        }

    }

    private List<TupleDescItem> tupleDescItems;

    public List<TupleDescItem> getTupleDescItems() {
        return tupleDescItems;
    }

    public void setTupleDescItems(List<TupleDescItem> items) {
        this.tupleDescItems = items;
    }

    /**
     * @return 返回该TupleDesc对象所有字段TupleDescItems的迭代器
     */
    public Iterator<TupleDescItem> iterator() {
        return tupleDescItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * TupleDesc构造函数
     * @param fieldTypes 字段（field）类型数组
     * @param fieldNames 字段（field）名称数组，name可以为null
     */
    public TupleDesc(Type[] fieldTypes, String[] fieldNames) {
        tupleDescItems = new ArrayList<>();
        int fieldNum = fieldTypes.length;
        for (int i=0; i<fieldNum; i++) {
            TupleDescItem item = new TupleDescItem(fieldTypes[i], fieldNames[i]);
            tupleDescItems.add(item);
        }
    }

    public TupleDesc() {}

    /**
     * TupleDesc构造函数
     * @param fieldTypes 字段（field）类型数组
     */
    public TupleDesc(Type[] fieldTypes) {
        tupleDescItems = new ArrayList<>();
        int fieldNum = fieldTypes.length;
        for (Type fieldType : fieldTypes) {
            TupleDescItem item = new TupleDescItem(fieldType, null);
            tupleDescItems.add(item);
        }
    }

    /**
     * @return 返回字段（fields）的数量
     */
    public int getFieldsNum() {
        return tupleDescItems.size();
    }

    /**
     * 获取第index个字段名称（可能为null）
     * @param index 字段名称的索引值
     * @return 第index个字段的名称
     * @throws NoSuchElementException 如果index不是一个有效的field索引
     */
    public String getFieldName(int index) throws NoSuchElementException {
        // 索引越界抛出异常
        if (index < 0 || index > tupleDescItems.size()) {
            throw new NoSuchElementException("index " + index + " is not valid");
        }
        return tupleDescItems.get(index).fieldName;
    }

    /**
     * 获取该TupleDesc对象的第index个字段的类型
     * @param index 字段类型的索引值
     * @return 第index个字段的类型
     * @throws NoSuchElementException 如果index不是一个有效的field索引
     */
    public Type getFieldType(int index) throws NoSuchElementException {
        // 索引越界抛出异常
        if (index < 0 || index > tupleDescItems.size()) {
            throw new NoSuchElementException("index " + index + " is not valid");
        }
        return tupleDescItems.get(index).fieldType;
    }

    /**
     * 根据字段名称获取对应的索引值
     * @param fieldName 字段名称
     * @return 字段名称对应的索引值
     * @throws NoSuchElementException 没有匹配的字段名称
     */
    public int fieldNameToIndex(String fieldName) throws NoSuchElementException {
        if (fieldName == null) {
            throw new NoSuchElementException("field name is null");
        }
        for (int i=0; i<tupleDescItems.size(); i++) {
            if (fieldName.equals(getFieldName(i))) {
                return i;
            }
        }
        // 没找到对应的fieldName，抛出异常
        throw new NoSuchElementException("field name " + fieldName + " is not founded");
    }

    /**
     * @return 返回元组（Tuple）的字节大小
     */
    public int getSize() {
        int bytes = 0;
        for (TupleDescItem item: tupleDescItems) {
            bytes += item.fieldType.getLen();
        }
        return bytes;
    }

    /**
     * 合并两个TupleDesc对象
     * 新对象的字段数（fieldNum）为：td1.getFieldsNum() + td2.getFieldsNum()
     * @param td1 TupleDesc对象
     * @param td2 TupleDesc对象
     * @return 新TupleDesc对象
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TupleDescItem> items = new ArrayList<>();
        items.addAll(td1.getTupleDescItems());
        items.addAll(td2.getTupleDescItems());
        TupleDesc td = new TupleDesc();
        td.setTupleDescItems(items);
        return td;
    }

    /**
     * 用于与该TupleDesc进行比较
     * 两个TupleDesc如果拥有相同的字段数量，并且每个字段的类型（Type）均相同，则认为相同
     * @param obj 用于与该TupleDesc进行比较的Object对象
     * @return 相同则返回true，反之为false
     */
    @Override
    public boolean equals(Object obj) {
        // 如果obj的类不是TupleDesc则不相同，返回false
        if (!this.getClass().isInstance(obj)) {
            return false;
        }
        TupleDesc td = (TupleDesc) obj;
        int len = this.tupleDescItems.size();
        // 判断两者的tupleDescItems是否具有相同的元素个数
        if (len != td.tupleDescItems.size()) {
            return false;
        }
        // 逐个元素比较fieldType是否相同
        for (int i=0; i<len; i++) {
            if (this.tupleDescItems.get(i).fieldType != td.tupleDescItems.get(i).fieldType)
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // 相同TupleDesc相同对象具有相同的hashCode
        int magicNum = 2333333;
        int code = 0;
        int len = tupleDescItems.size();
        for (int i=0; i<len; i++) {
            Type type = tupleDescItems.get(i).fieldType;
            if (type.equals(Type.INT_TYPE)) {
                code += i * i * 4 % magicNum;
            } else if (type.equals(Type.STRING_TYPE)) {
                code += i * i * 7 % magicNum;
            }
        }
        return code;
    }

    /**
     * 格式："fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])"
     * @return 描述该TupleDesc的字符串
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        int len = this.tupleDescItems.size();
        for (int i=0; i<len; i++) {
            TupleDescItem item = this.tupleDescItems.get(i);
            str.append(item.fieldType).append("[").append(i).append("]").append(item.fieldName).append("[").append(i).append("]");
        }
        return str.toString();
    }
}
