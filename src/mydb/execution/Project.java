package mydb.execution;

import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;

import java.io.Serial;
import java.util.*;

/**
 * 投影操作符
 */
public class Project extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private final TupleDesc tupleDesc;

    /**
     * 进行投影输出的字段
     */
    private final List<Integer> outFields;

    /**
     * 投影（Project）操作符的构造函数<br>
     * 接受一个子操作符（child）用于读取元组并进行投影，和一个用于输出元组的列表
     * @param fieldList 需要进行投影输出的字段索引列表
     * @param typeList 投影字段对应的字段类型
     * @param child 子操作符
     */
    public Project(List<Integer> fieldList, List<Type> typeList, OpIterator child) {
        this(fieldList, typeList.toArray(new Type[]{}), child);
    }

    public Project(List<Integer> fieldList, Type[] types, OpIterator child) {
        this.child = child;
        this.outFields = fieldList;
        TupleDesc childTupleDesc = child.getTupleDesc();
        String[] fieldNames = new String[fieldList.size()];
        for (int i=0; i<fieldNames.length; i++) {
            fieldNames[i] = childTupleDesc.getFieldName(fieldList.get(i));
        }
        this.tupleDesc = new TupleDesc(types, fieldNames);
    }

    public void open() throws DbException, TransactionException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionException {
        this.child.rewind();
    }

    /**
     * 遍历child操作符的每一个元组，然后进行投影（project）
     * @return 返回下一个元组，如果后面没有元组了则返回null
     */
    @Override
    protected Tuple fetchNext()
            throws DbException, NoSuchElementException, TransactionException {
        if (!child.hasNext()) {
            return null;
        }
        Tuple childTuple = child.next();
        Tuple newTuple = new Tuple(this.tupleDesc);
        for (int i=0; i< tupleDesc.getFieldsNum(); i++) {
            newTuple.setField(i, childTuple.getField(outFields.get(i)));
            newTuple.setRecordId(childTuple.getRecordId()); // FIXME
        }
        return newTuple;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
