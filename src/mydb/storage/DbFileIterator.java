package mydb.storage;

import mydb.common.DbException;
import mydb.transaction.TransactionException;

import java.util.*;

/**
 * DbFileIterator是数据库文件（DbFile）的接口
 */
public interface DbFileIterator {

    void open() throws DbException, TransactionException;

    void close() throws DbException;

    /**
     * @return 如果还有下一个元组则返回true，否则返回false
     * 如果迭代器未open也会返回false
     */
    boolean hasNext() throws DbException, TransactionException;

    /**
     * 获取下一个元组（tuple）
     * @return 返回迭代器的下一个元组
     */
    Tuple next() throws DbException, NoSuchElementException, TransactionException;

    /**
     * 设置迭代器指针为起始位置，可以重新进行迭代
     * @throws DbException 如果不支持rewind会抛出异常
     */
    void rewind() throws DbException, TransactionException;

}
