package mydb.storage;

import java.io.Serial;

import mydb.common.DbException;
import mydb.execution.OpIterator;

import java.util.*;

/**
 * 元组（Tuple）的迭代器（Iterator）
 */
public class TupleIterator implements OpIterator {

    @Serial
    private static final long serialVersionUID = 1L;

    Iterator<Tuple> tupleIterator = null;
    TupleDesc tupleDesc = null;
    Iterable<Tuple> tuples = null;

    /**
     * TupleIterator构造函数
     * @param td 元组描述符TupleDesc（tuple describer）
     * @param tuples 需要迭代的元组（Tuple）集合
     */
    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.tupleDesc = td;
        this.tuples = tuples;

        // 检查tuples中所有tuple是否为正确的TupleDesc
        for (Tuple t: tuples) {
            if (!t.getTupleDesc().equals(td)) {
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
            }
        }
    }

    @Override
    public void open() {
        tupleIterator = tuples.iterator();
    }

    @Override
    public void close() {
        tupleIterator = null;
    }

    @Override
    public boolean hasNext() {
        return tupleIterator.hasNext();
    }

    @Override
    public Tuple next() {
        return tupleIterator.next();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    @Override
    public void rewind() throws DbException {
        // 先close在open以初始化迭代器
        close();
        open();
    }
}
