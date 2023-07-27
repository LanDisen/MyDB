package java.mydb.execution;

import java.io.Serial;
import java.mydb.common.Catalog;
import java.mydb.common.Database;
import java.mydb.common.DbException;
import java.mydb.storage.DbFile;
import java.mydb.storage.DbFileIterator;
import java.mydb.storage.Tuple;
import java.mydb.storage.TupleDesc;
import java.mydb.transaction.TransactionId;

import java.util.*;

/**
 * SeqScan（sequential scan）类是一种顺序扫描方法的实现
 * 它不一定按特定顺序读取表中的每个元组
 */
public class SeqScan implements OpIterator {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Catalog catalog;
    private TupleDesc tupleDesc;
    private final TransactionId tid;
    private int tableId;
    private String tableName;
    private String tableAlias;
    private DbFile dbFile;
    private DbFileIterator iterator;

    /**
     * SeqScan的构造函数
     * @param tid 事务ID
     * @param tableId 需要进行扫描的表的ID
     * @param tableAlias 表的别名。返回的tupleDesc字段为：tableAlias.filedName
     *                   （可以是null.fieldName、tableAlias.null、null.null）
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        this.tid = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        this.catalog = Database.getCatalog();
        this.tupleDesc = getAliasTupleDesc(tupleDesc, tableAlias);
        this.tableName = catalog.getTableName(tableId);
        this.dbFile = catalog.getDbFile(tableId);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    private TupleDesc getAliasTupleDesc(TupleDesc td, String alias) {
        TupleDesc newTupleDesc = new TupleDesc();
        List<TupleDesc.TupleDescItem> newItems = new ArrayList<>();
        List<TupleDesc.TupleDescItem> tdItems = td.getTupleDescItems();
        for (TupleDesc.TupleDescItem tdItem: tdItems) {
            newItems.add(new TupleDesc.TupleDescItem(tdItem.fieldType, alias + "." + tdItem.fieldName));
        }
        newTupleDesc.setTupleDescItems(newItems);
        return newTupleDesc;
    }

    /**
     * @return 返回该操作符扫描到的表的名称（表的实际名称，而不是别名）
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return 返回该操作符扫描到的表的别名
     */
    public String getTableAlias() {
        return tableAlias;
    }

    /**
     * 重新设置表的ID和别名
     */
    public void resetTable(int tableId, String tableAlias) {
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        this.tupleDesc = getAliasTupleDesc(catalog.getTupleDesc(tableId), tableAlias);
        this.tableName = catalog.getTableName(tableId);
        this.dbFile = catalog.getDbFile(tableId);
        try {
            open();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open() throws DbException {
        this.iterator = this.dbFile.iterator(tid);
        this.iterator.open();
    }

    @Override
    public void close() throws DbException  {
        iterator = null;
    }

    @Override
    public boolean hasNext() throws DbException  {
        if (iterator == null) {
            return true;
        }
        return iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, NoSuchElementException{
        if (iterator == null) {
            throw new NoSuchElementException("No next tuple");
        }
        Tuple tuple = iterator.next();
        if (tuple == null) {
            throw new NoSuchElementException("No next tuple");
        }
        return tuple;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }
}
