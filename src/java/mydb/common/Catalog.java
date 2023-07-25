package java.mydb.common;

import java.mydb.common.Type;
import java.mydb.storage.DbFile;
import java.mydb.storage.TupleDesc;
import java.mydb.storage.Table;

import java.mydb.storage.DbFile;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Catalog类用来跟踪数据库中的表（tables）和它们对应的模式（schemas）
 */
public class Catalog {

//    public class Table {
//        private DbFile dbFile;
//        private String name;
//        private String primaryKeyName;
//
//        public Table(DbFile dbFile, String name, String primaryKeyName) {
//            this.dbFile = dbFile;
//            this.name = name;
//            this.primaryKeyName = primaryKeyName;
//        }
//    }

    /**
     * 通过DbFile的getId()作为key获得对应的Table
     */
    private Map<Integer, Table> tables;

    /**
     * 表名作为key获得对应的table id
     */
    private Map<String, Integer> tableName2Id;

    /**
     * Catalog构造函数，创建一个空的目录对象
     */
    public Catalog() {
        tables = new ConcurrentHashMap<>();
        tableName2Id = new ConcurrentHashMap<>();
    }

    /**
     * 添加一个新表到catalog中
     * @param dbFile 数据库文件
     * @param tableName 表的名称
     * @param primaryKeyName 主键（primary key）名称
     */
    public void addTable(DbFile dbFile, String tableName, String primaryKeyName) {
        Table table = new Table(dbFile, tableName, primaryKeyName);
        tables.put(dbFile.getId(), table);
        tableName2Id.put(tableName, dbFile.getId());
    }

    public void addTable(DbFile dbFile, String tableName) {
        addTable(dbFile, tableName, "");
    }

    public void addTable(DbFile dbFile) {
        addTable(dbFile, (UUID.randomUUID()).toString());
    }

    /**
     * @return 返回对应表的ID
     * @throws NoSuchElementException 如果表不存在
     */
    public int getTableId(String tableName) throws NoSuchElementException {
        if (tableName == null || tableName2Id.containsKey(tableName)) {
            throw new NoSuchElementException("A table named " + tableName + " is not found");
        }
        return tableName2Id.get(tableName);
    }

    /**
     * @param tableId 表的ID，该参数可使用DbFile.getId()得到
     * @return 返回对应表的TupleDesc（schema）
     * @throws NoSuchElementException 如果表不存在
     */
    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        if (tables.containsKey(tableId))
            return tables.get(tableId).getDbFile().getTupleDesc();
        throw new NoSuchElementException("Table " + tableId + " is not found");
    }

    /**
     * @param tableId 表的ID，该参数可使用DbFile.getId()得到
     */
    public DbFile getDbFile(int tableId) throws NoSuchElementException {
        if (tables.containsKey(tableId))
            return tables.get(tableId).getDbFile();
        throw new NoSuchElementException("Table " + tableId + " is not found");
    }

    /**
     *
     * @param tableId 表的ID，该参数可使用DbFile.getId()得到
     * @return 返回表的主键（primary key）
     */
    public String getPrimaryKey(int tableId) throws NoSuchElementException {
        Table table = tables.get(tableId);
        if (table == null)
            throw new NoSuchElementException("Table " + tableId + " is not found");
        return table.getPrimaryKeyName();
    }

    public Iterator<Integer> tableIdIterator() {
        return tables.keySet().iterator();
    }

    public String getTableName(int tableId) throws NoSuchElementException {
        if (tables.get(tableId) == null)
            throw new NoSuchElementException("Table " + tableId + " is not found");
        return tables.get(tableId).getName();
    }

    /**
     * 删除该目录（catalog）的所有表
     */
    public void clear() {
        tables.clear();
        tableName2Id.clear();
    }

//    /**
//     * 从catalogFile中读取schema并在数据库中创建对应的表
//     */
//    public void loadSchema(String catalogFile) {
//        String line = "";
//        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.exit(0);
//        }
//    }


}
