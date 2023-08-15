package mydb.execution;

import mydb.common.DbException;
import mydb.storage.Field;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;

import java.io.Serial;
import java.util.*;

/**
 * 排序操作符
 */
public class OrderBy extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private final List<Tuple> childTuples = new ArrayList<>();

    private final TupleDesc tupleDesc;

    private final int orderByFieldIndex;

    private final String orderByFieldName;

    /**
     * 用于遍历已排序的元组集合
     */
    private Iterator<Tuple> tupleIterator;

    private final boolean asc;

    /**
     * OrderBy操作符的构造函数
     * @param orderByFieldIndex 进行排序的字段索引
     * @param asc true为升序排序，false为降序排序
     * @param child 需要进行排序的元组集合（OpIterator）
     */
    public OrderBy(int orderByFieldIndex, boolean asc, OpIterator child) {
        this.child = child;
        this.tupleDesc = child.getTupleDesc();
        this.orderByFieldIndex = orderByFieldIndex;
        this.orderByFieldName = tupleDesc.getFieldName(orderByFieldIndex);
        this.asc = asc;
    }

    public boolean isAsc() {
        return this.asc;
    }

    public int getOrderByFieldIndex() {
        return this.orderByFieldIndex;
    }

    public String getOrderByFieldName() {
        return this.orderByFieldName;
    }

    public void open() throws DbException, NoSuchElementException, TransactionException {
        this.child.open();
        while (child.hasNext()) {
            childTuples.add(child.next());
        }
        // 参数为需要进行排序的字段索引和顺序标志
        childTuples.sort(new TupleComparator(orderByFieldIndex, asc));
        tupleIterator = childTuples.iterator();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        tupleIterator = null;
    }

    @Override
    public void rewind() throws DbException, TransactionException {
        tupleIterator = childTuples.iterator();
    }

    /**
     * @return 返回下一个已排序的元组，若后面没有元组则返回null
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException, TransactionException {
        if (tupleIterator == null || !tupleIterator.hasNext()) {
            return null;
        }
        return tupleIterator.next();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }


    /**
     * 排序操作需要对元组（Tuple）的某个字段进行比较
     */
    class TupleComparator implements Comparator<Tuple> {

        final int fieldIndex;

        final boolean asc;

        public TupleComparator(int fieldIndex, boolean asc) {
            this.fieldIndex = fieldIndex;
            this.asc = asc;
        }

        /**
         * @return 0为相等，1为大于，-1为小于
         */
        @Override
        public int compare(Tuple o1, Tuple o2) {
            Field field1 = o1.getField(fieldIndex);
            Field field2 = o2.getField(fieldIndex);
            if (field1.compare(Predicate.Op.EQUALS, field2)) {
                return 0;
            }
            if (field1.compare(Predicate.Op.GREATER_THAN, field2)) {
                return asc ? 1 : -1;
            }
            // LESS_THAN
            return asc ? -1 : 1;
        }
    }
}

