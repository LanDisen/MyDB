package mydb.execution;

import mydb.common.DbException;
import mydb.storage.Field;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;

import java.io.Serial;
import java.util.*;

/**
 * JOIN：连接操作符
 */
public class Join extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    private final JoinPredicate joinPredicate;
    private OpIterator[] children;
    private Tuple tuple1;

    /**
     * Join构造函数，接收两个用于JOIN的子操作符迭代器，对元组进行JOIN操作
     * @param joinPredicate JOIN谓词
     * @param child1 左（第一个）操作符迭代器
     * @param child2 右（第二个）操作符迭代器
     */
    public Join(JoinPredicate joinPredicate, OpIterator child1, OpIterator child2) {
        this.joinPredicate = joinPredicate;
        this.children = new OpIterator[]{child1, child2};
        this.tuple1 = null;
    }

    public JoinPredicate getJoinPredicate() {
        return joinPredicate;
    }

    /**
     * @return 第一个字段的字段名，前缀应该包括表名或表的别名
     */
    public String getJoinField1Name() {
        return children[0].getTupleDesc().getFieldName(joinPredicate.getFieldIndex1());
    }

    /**
     * @return 第二个字段的字段名，前缀应该包括表名或表的别名
     */
    public String getJoinField2Name() {
        return children[1].getTupleDesc().getFieldName(joinPredicate.getFieldIndex2());
    }

    public void open()
            throws DbException, NoSuchElementException, TransactionException {
        for (OpIterator child: children) {
            child.open();
        }
        super.open();
    }

    public void close() {
        for (OpIterator child: children) {
            child.close();
        }
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionException {
        for (OpIterator child : this.children) {
            child.rewind();
        }
    }

    /**
     * 返回通过JOIN得到的下一个元组，如果没有下一个元组则返回null
     * 例如：一个元组为{1,2,3}，另一个元组为{1，5，6}
     * 使用对第一个字段使用等值连接（JOIN）得到{1,2,3,1,5,6}
     * @return 返回下一个匹配的元组，若无下一个元组则返回null
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException, TransactionException {
        while (children[0].hasNext() || tuple1 != null) {
            if (children[0].hasNext() && tuple1 == null) {
                tuple1 = children[0].next();
            }
            while (children[1].hasNext()) {
                Tuple tuple2 = children[1].next();
                // 对满足谓词条件的元组进行JOIN
                if (joinPredicate.filter(tuple1, tuple2)) {
                    TupleDesc tupleDesc = getTupleDesc();
                    Tuple newTuple = new Tuple(tupleDesc);
                    int i = 0;
                    Iterator<Field> fields1 = tuple1.fields();
                    while (fields1.hasNext() && i < tupleDesc.getFieldsNum()) {
                        newTuple.setField(i++, fields1.next());
                    }
                    Iterator<Field> fields2 = tuple2.fields();
                    while (fields2.hasNext() && i < tupleDesc.getFieldsNum()) {
                        newTuple.setField(i++, fields2.next());
                    }
                    return newTuple;
                }
            }
            // nested loop，重新循环第二个元组集合
            children[1].rewind();
            tuple1 = null;
        }
        return null;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(children[0].getTupleDesc(), children[1].getTupleDesc());
    }

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }
}
