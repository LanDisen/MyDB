package mydb.execution;

import java.io.Serial;

import mydb.common.DbException;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;

import java.util.NoSuchElementException;

/**
 * 操作符（Operator）的抽象类
 */
public abstract class Operator implements OpIterator {

    @Serial
    private static final long serialVersionUID = 1L;

    private Tuple next = null;
    private boolean isOpen = false;

    /**
     * 该操作符估计的基数
     */
    private int cardinality = 0;

    @Override
    public void open() throws DbException, TransactionException {
        this.isOpen = true;
    }

    @Override
    public void close() {
        this.next = null;
        this.isOpen = false;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionException {
        if (this.next == null) {
            this.next = fetchNext();
        }
        return (this.next != null);
    }

    @Override
    public Tuple next() throws DbException, TransactionException {
        if (this.next == null) {
            this.next = fetchNext();
            if (this.next == null) {
                throw new NoSuchElementException();
            }
        }
        Tuple tuple = this.next;
        this.next = null;
        return tuple;
    }

    /**
     * @return 返回迭代器的下一个Tuple（如果迭代结束则返回null）
     */
    protected abstract Tuple fetchNext()
            throws DbException, NoSuchElementException, TransactionException;

    /**
     * @return 返回该操作符对应tuple的TupleDesc
     */
    public abstract TupleDesc getTupleDesc();

    /**
     * @return 返回该操作符的子操作符迭代器（children）
     */
    public abstract OpIterator[] getChildren();

    public abstract void setChildren(OpIterator[] children);

    /**
     * @return 返回该操作符的基数
     */
    public int getEstimatedCardinality() {
        return this.cardinality;
    }

    /**
     * 设置该操作符所估计的基数
     */
    public void setEstimatedCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

}
