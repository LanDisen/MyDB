package mydb.execution;

import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;

import java.io.Serial;
import java.util.*;

/**
 * 聚合操作符，用于对COUNT、SUM、AVG、MIN、MAX等进行聚合操作（需要GROUP BY）
 * 目前仅支持对单个字段的聚合操作
 */
public class Aggregate extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aggregateField;
    private int groupField;
    private Aggregator.Op aggregatorOp;
    private Aggregator aggregator;
    private OpIterator opIterator;

    /**
     * 聚合操作符（Aggregate）的构造函数
     * @param child 子操作符迭代器，用于获得需要进行聚合操作的元组
     * @param aggregateField 需要进行聚合的字段
     * @param groupField 已经进行聚合分组的字段
     * @param aggregatorOp 使用的聚合操作符（MAX、MIN、AVG等）
     */
    public Aggregate(OpIterator child, int aggregateField, int groupField,
                     Aggregator.Op aggregatorOp) {
        this.child = child;
        this.aggregateField = aggregateField;
        this.groupField = groupField;
        this.aggregatorOp = aggregatorOp;
        // 进行聚合操作的字段类型
        Type fieldType = child.getTupleDesc().getFieldType(aggregateField);
        // 进行分组操作的字段类型
        Type groupType = (groupField == -1 ? null : child.getTupleDesc().getFieldType(groupField));
        if (fieldType == Type.INT_TYPE) {
            aggregator = new IntAggregator(groupField, groupType, aggregateField, aggregatorOp);
        } else {
            aggregator = new StrAggregator(groupField, groupType, aggregateField, aggregatorOp);
        }
        this.opIterator = null;
    }

    public int getAggregateField() {
        return aggregateField;
    }

    public String getAggregateFieldName() {
        if (groupField == -1) {
            return null;
        }
        return child.getTupleDesc().getFieldName(aggregateField);
    }

    public int getGroupField() {
        return groupField;
    }

    public String getGroupFieldName() {
        return child.getTupleDesc().getFieldName(groupField);
    }

    public Aggregator.Op getAggregateOp() {
        return aggregatorOp;
    }

    public static String getAggregateOpName(Aggregator.Op aggregatorOp) {
        return aggregatorOp.toString();
    }

    public void open()
            throws DbException, NoSuchElementException, TransactionException {
        super.open();
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        opIterator = aggregator.iterator();
        opIterator.open();
    }

    public void close() {
        super.close();
        aggregator = null;
        opIterator = null;
    }

    @Override
    public void rewind() throws DbException, TransactionException {
        opIterator.rewind();
    }

    /**
     * 返回下一个元组。如果按字段分组，第一个字段为分组字段，第二个字段为计算聚合的结果。
     * 如果没有按字段分组，则返回的元组应该包含一个表示聚合结果的字段。
     * 若没有更多元组则返回null。
     * 指定分组时，返回结果格式为：(groupValue, aggregateValue)
     * 未指定分组时，返回结果格式为：(aggregateValue)
     */
    @Override
    protected Tuple fetchNext()
            throws DbException, NoSuchElementException, TransactionException {
        if (opIterator.hasNext()) {
            return opIterator.next();
        }
        return null;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return aggregator.getTupleDesc();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
        // 需要重新设置aggregator
        // 进行聚合操作的字段类型
        Type fieldType = child.getTupleDesc().getFieldType(aggregateField);
        // 进行分组操作的字段类型
        Type groupType = (groupField == -1 ? null : child.getTupleDesc().getFieldType(groupField));
        if (fieldType == Type.INT_TYPE) {
            aggregator = new IntAggregator(groupField, groupType, aggregateField, aggregatorOp);
        } else {
            aggregator = new StrAggregator(groupField, groupType, aggregateField, aggregatorOp);
        }
    }
}
