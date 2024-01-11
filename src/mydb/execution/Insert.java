package mydb.execution;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.BufferPool;
import mydb.storage.IntField;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;
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
    private Tuple insertedTuple; // 需要进行插入操作的元组

    /**
     * Insert构造函数
     * @param tid 事务ID
     * @param child 用于遍历需要插入的元组（Tuple）列表的OpIterator
     * @param tableId 元组所插入的表ID
     * @throws DbException 若child的TupleDesc与需要插入的元组的TupleDesc不符合会抛出异常
     */
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        this.tid = tid;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(
                new Type[] {Type.INT_TYPE}, new String[] {"insertedNum"});
        // this.tupleDesc = child.getTupleDesc();
        this.insertedTuple = null;
    }

    public void open() throws DbException, TransactionException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionException {
        child.rewind();
    }

    /**
     * 从child中得到需要插入的元组，插入到构造函数指定的表中。
     * 插入元组前无需检查元组是否重复
     * @return 返回一个具有一个字段的元组，其包含已插入的记录数量。若该方法被重复调用则返回null
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException, TransactionException {
        // 该方法只需调用一次
        if (insertedTuple != null) {
            // 该方法已被重复调用
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int insertedTuplesNum = 0; // 记录已进行插入操作的元组数量
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
