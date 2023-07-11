package java.mydb.common;

import java.mydb.common.Type;
import java.mydb.storage.DbFile;
import java.mydb.storage.TupleDesc;

import java.mydb.storage.DbFile;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Catalog类用来跟踪数据库中的表（tables）和它们对应的模式（schemas）
 */
public class Catalog {

    /**
     * Catalog构造函数，创建一个空的目录对象
     */
    public Catalog() {
        // TODO
    }

    /**
     * 添加一个新表到catalog中
     * @param dbFile 数据库文件
     * @param tableName 表的名称
     * @param primaryKeyName 主键（primary key）名称
     */
    public void addTable(DbFile dbFile, String tableName, String primaryKeyName) {
        // TODO
    }

    public void addTable(DbFile dbFile, String tableName) {
        addTable(dbFile, tableName, "");
    }

//    public void addTable(DbFile dbFile) {
//
//    }

    /**
     * @return 返回对应表的ID
     * @throws NoSuchElementException 如果表不存在
     */
    public int getTableId(String tableName) throws NoSuchElementException {
        // TODO
        return 0;
    }

    /**
     *
     * @param tableId 表的ID，该参数可使用DbFile.getId()得到
     * @return 返回对应表的TupleDesc（schema）
     * @throws NoSuchElementException 如果表不存在
     */
    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        // TODO
        return null;
    }

    /**
     * @param tableId 表的ID，该参数可使用DbFile.getId()得到
     */
    public DbFile getDbFile(int tableId) throws NoSuchElementException {
        // TODO
        return null;
    }

    /**
     *
     * @param tableId 表的ID，该参数可使用DbFile.getId()得到
     * @return 返回表的主键（primary key）
     */
    public String getPrimaryKey(int tableId) {
        // TODO
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // TODO
        return null;
    }

    public String getTableName(int tableId) {
        // TODO
        return null;
    }

    /**
     * 删除该目录（catalog）的所有表
     */
    public void clear() {
        // TODO
    }

//    public void loadSchema(String catalogFile) {
//
//    }


}
