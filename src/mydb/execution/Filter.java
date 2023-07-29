package mydb.execution;

import mydb.common.DbException;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;

import java.io.Serial;
import java.util.*;

/**
 * 过滤器操作符，用于筛选符合条件的元组
 */
public class Filter extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator child;

    /**
     * 过滤器构造函数
     * @param predicate 谓词
     * @param child 子操作符
     */
    public Filter(Predicate predicate, OpIterator child) {
        this.predicate = predicate;
        this.child = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public void open()
            throws DbException, NoSuchElementException {
        super.open();
        child.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    /**
     * 可用于遍历子操作符的元组
     * @return 返回下一个元组，若没有下一个元组则返回null
     */
    @Override
    protected Tuple fetchNext()
            throws DbException, NoSuchElementException {
        while (this.child.hasNext()) {
            Tuple tuple = child.next();
            if (this.predicate.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
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
