package mydb.execution;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.BufferPool;
import mydb.storage.IntField;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionId;

import java.io.IOException;
import java.io.Serial;
import java.util.*;

public class Delete extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final TupleDesc tupleDesc;
    private Tuple deletedTuple; // ����ɾ��������Ԫ��

    /**
     * Delete���캯��
     * @param tid ����ID
     * @param child ���ڶ�ȡ��Ҫɾ����Ԫ���OpIterator
     */
    public Delete(TransactionId tid, OpIterator child)
            throws DbException {
        this.tid = tid;
        this.child = child;
        this.tupleDesc = new TupleDesc(
                new Type[] {Type.INT_TYPE}, new String[] {"deletedNum"});
        this.deletedTuple = null;
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
     * ��child�л����һ����Ҫɾ����Ԫ��
     * @return ���ص�һ�ֶ�Ԫ�飬�������ɾ���ļ�¼����
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException {
        // �÷���ֻ�����һ��
        if (deletedTuple != null) {
            // �÷����ѱ��ظ�����
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int deletedTuplesNum = 0;
        while (child.hasNext()) {
            try {
                bufferPool.deleteTuple(tid, child.next());
                deletedTuplesNum++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        deletedTuple = new Tuple(tupleDesc);
        deletedTuple.setField(0, new IntField(deletedTuplesNum));
        return deletedTuple;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
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
