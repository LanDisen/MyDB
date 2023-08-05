package mydb.storage;

import mydb.common.DbException;
import mydb.common.Catalog;
import mydb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * 磁盘中数据库文件（database file）的接口，每一个表（table）都作为一个DbFile进行存储
 * DbFile可以获取页面（page）并且可以遍历元组（tuples）
 */
public interface DbFile {

    /**
     * 从磁盘中读取对应页面
     */
    Page readPage(PageId pid);

    /**
     * 将指定页面写入磁盘
     */
    void writePage(Page page) throws IOException;

    /**
     * 指定事务将元组插入到指定的数据库文件中，写文件时需要请求写锁
     * @param tid 进行插入操作的事务ID
     * @param tuple 需要插入的元组
     * @return 返回进行了修改的页面列表
     * @throws DbException 如果无法插入元组会抛出数据库异常
     * @throws IOException 如果指定的文件不可读或写会抛出IO异常
     */
    List<Page> insertTuple(TransactionId tid, Tuple tuple)
            throws DbException, IOException;

    /**
     * 指定事务在指定的数据库文件上删除元组，写文件时需要请求写锁
     * @param tid 事务ID
     * @param tuple 需要进行删除的元组
     * @return 返回进行了修改的页面列表
     * @throws DbException 如果元组无法被删除，或者不属于指定的数据库文件时会抛出数据库异常
     */
    List<Page> deleteTuple(TransactionId tid, Tuple tuple)
            throws DbException, IOException;

    /**
     * @return 返回DbFIle在Catalog中唯一的ID
     */
    int getId();

    /**
     * @return 返回该DbFile的TupleDesc
     */
    TupleDesc getTupleDesc();

    /**
     * @return 返回用于遍历该数据库文件的元组的迭代器
     */
    DbFileIterator iterator(TransactionId tid);

}
