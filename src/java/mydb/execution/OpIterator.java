package java.mydb.execution;

import java.mydb.common.DbException;
import java.mydb.storage.Tuple;
import java.mydb.storage.TupleDesc;

import java.io.Serializable;
import java.util.*;

/**
 * OpIterator是MyDB中所有操作符（Operator）需要实现的迭代器（Iterator）接口
 */
public interface OpIterator extends Serializable {

    /**
     * 打开迭代器
     */
    void open() throws DbException;

    /**
     * 关闭迭代器
     */
    void close() throws DbException;

    /**
     * 判断迭代器是否还有下一个元组（Tuple）
     * @return 如果有下一个元组则返回true，反之false
     */
    boolean hasNext() throws DbException;

    /**
     * @return 返回迭代器的下一个元组（Tuple）
     */
    Tuple next() throws DbException, NoSuchElementException;

    /**
     * @return 返回迭代器对应元组的TupleDesc
     */
    TupleDesc getTupleDesc();

}
