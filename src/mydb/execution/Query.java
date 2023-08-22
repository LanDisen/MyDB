package mydb.execution;

import mydb.common.DbException;
import mydb.optimizer.LogicalPlan;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;
import mydb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * Query类用于管理数据库中的所有查询请求，用于获取一个查询计划并执行
 */
public class Query implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    transient private OpIterator opIterator;

    transient private LogicalPlan logicalPlan;

    final TransactionId tid;

    transient private boolean started = false;

    public Query(OpIterator root, TransactionId tid) {
        this.opIterator = root;
        this.tid = tid;
    }

    public void start() throws DbException, TransactionException {
        this.opIterator.open();
        started = true;
    }

    /**
     * 执行查询请求
     */
    public void execute()
            throws DbException, TransactionException {
        TupleDesc tupleDesc = getOutputTupleDesc();
        StringBuilder fieldNames = new StringBuilder();
        for (int i=0; i<tupleDesc.getFieldsNum(); i++) {
            fieldNames.append(tupleDesc.getFieldName(i)).append("\t");
        }
        System.out.println(fieldNames);
        for (int i=0; i<fieldNames.length() + tupleDesc.getFieldsNum()*4; i++) {
            System.out.print("-");
        }
        System.out.println();
        this.start(); // 开启执行查询请求
        int rows = 0;
        while (this.hasNext()) {
            Tuple tuple = this.next();
            System.out.println(tuple);
            rows++;
        }
        System.out.println("\n " + rows + " rows.");
        this.close();
    }

    public TransactionId getTransactionId() {
        return this.tid;
    }

    public void setLogicalPlan(LogicalPlan logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    public LogicalPlan getLogicalPlan() {
        return this.logicalPlan;
    }

    public void setPhysicalPlan(OpIterator opIterator) {
        this.opIterator = opIterator;
    }

    public OpIterator getPhysicalPlan() {
        return this.opIterator;
    }

    public TupleDesc getOutputTupleDesc() {
        return this.opIterator.getTupleDesc();
    }

    public boolean hasNext()
            throws DbException, TransactionException {
        if (!started) {
            throw new DbException("Database is not started.");
        }
        return opIterator.hasNext();
    }

    public Tuple next()
            throws DbException, NoSuchElementException, TransactionException {
        if (!started) {
            throw new DbException("Database is not started.");
        }
        return opIterator.next();
    }

    public void close() {
        opIterator.close();
        started=false;
    }
}
