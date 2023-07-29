package mydb.execution;

import mydb.common.DbException;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;

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
    void close();

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
     * 重新将迭代指针设为起始位置，重新进行迭代
     */
    void rewind() throws DbException;

    /**
     * @return 返回迭代器对应元组的TupleDesc
     */
    TupleDesc getTupleDesc();

}
