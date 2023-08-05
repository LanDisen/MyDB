package mydb.execution;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.BufferPool;
import mydb.storage.IntField;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionId;

import java.io.Serial;
import java.io.IOException;
import java.util.*;

public class Insert extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private Tuple insertedTuple; // ��Ҫ���в��������Ԫ��

    /**
     * Insert���캯��
     * @param tid ����ID
     * @param child ���ڱ�����Ҫ�����Ԫ�飨Tuple���б��OpIterator
     * @param tableId Ԫ��������ı�ID
     * @throws DbException ��child��TupleDesc����Ҫ�����Ԫ���TupleDesc�����ϻ��׳��쳣
     */
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        this.tid = tid;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(
                new Type[] {Type.INT_TYPE}, new String[] {"insertedNum"});
        this.insertedTuple = null;
    }

    public void open() throws DbException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException {
        child.rewind();
    }

    /**
     * ��child�еõ���Ҫ�����Ԫ�飬���뵽���캯��ָ���ı��С�
     * ����Ԫ��ǰ������Ԫ���Ƿ��ظ�
     * @return ����һ������һ���ֶε�Ԫ�飬������Ѳ���ļ�¼���������÷������ظ������򷵻�null
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException {
        // �÷���ֻ�����һ��
        if (insertedTuple != null) {
            // �÷����ѱ��ظ�����
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int insertedTuplesNum = 0; // ��¼�ѽ��в��������Ԫ������
        while (child.hasNext()) {
            try {
                bufferPool.insertTuple(tid, tableId, child.next());
                insertedTuplesNum++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        insertedTuple = new Tuple(this.tupleDesc);
        insertedTuple.setField(0, new IntField(insertedTuplesNum));
        return insertedTuple;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
