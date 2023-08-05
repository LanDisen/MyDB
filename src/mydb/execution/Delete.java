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
    private Tuple deletedTuple; // 进行删除操作的元组

    /**
     * Delete构造函数
     * @param tid 事务ID
     * @param child 用于读取需要删除的元组的OpIterator
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
     * 从child中获得下一个需要删除的元组
     * @return 返回单一字段元组，其包括已删除的记录数量
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException {
        // 该方法只需调用一次
        if (deletedTuple != null) {
            // 该方法已被重复调用
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
