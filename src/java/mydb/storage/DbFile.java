package java.mydb.storage;

import java.mydb.common.Catalog;

import java.util.*;
import java.io.*;

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
    //void writePage(PageId pid) throws IOException;
    void writePage(Page page) throws IOException;

    /**
     * @return 返回DbFIle在Catalog中唯一的ID
     */
    int getId();

    /**
     * @return 返回该DbFile的TupleDesc
     */
    TupleDesc getTupleDesc();

    //DbFileIterator iterator(TransactionId tid);

}
