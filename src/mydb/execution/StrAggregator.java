package mydb.execution;

import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.Field;
import mydb.storage.IntField;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;

import java.io.Serial;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StrAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;
    private static final IntField NO_GROUP_BY = new IntField(-1);

    private int groupField;
    private Type groupFieldType;
    private int aggregateField;
    private Op op;
    private Map<Field, Tuple> tupleMap; // 聚合操作后的结果
    private TupleDesc tupleDesc;

    /**
     * StrAggregator构造函数
     * @param groupField 进行GROUP BY分组的字段索引，若不进行分组则为-1
     * @param groupFieldType 进行GROUP BY分组的字段类型，若不进行分组则为null
     * @param aggregateField 进行聚合操作的字段索引
     * @param op 聚合操作符（Aggregate.Op），str类型字段的聚合操作只能为COUNT
     */
    public StrAggregator(int groupField, Type groupFieldType, int aggregateField, Aggregator.Op op)  {
        // str类型的字段只能进行COUNT，而不能进行MIN、MAX、AVG、SUM等聚合操作
        if (!op.equals(Op.COUNT)) {
            throw new IllegalArgumentException();
        }
        this.groupField = groupField;
        this.groupFieldType = groupFieldType;
        this.aggregateField = aggregateField;
        this.op = op;
        this.tupleMap = new ConcurrentHashMap<>();
        if (groupField == -1) { // 不进行分组操作（GROUP BY）
            this.tupleDesc = new TupleDesc(
                    new Type[] {Type.INT_TYPE}, new String[] {"aggregateValue"});
            Tuple tuple = new Tuple(this.tupleDesc);
            tuple.setField(0, new IntField(0));
            this.tupleMap.put(NO_GROUP_BY, tuple);
        } else {
            this.tupleDesc = new TupleDesc(
                    new Type[] {groupFieldType, Type.INT_TYPE},
                    new String[] {"groupValue", "aggregateValue"});
        }
    }

    /**
     * 将新的元组合并到聚合操作中，按照构造函数的指示进行分组
     * @param tuple 需要进行聚合的元组
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tuple) {
        // str类型字段的聚合结果元组合并只需考虑COUNT聚合操作
        if (this.groupField == -1) {
            // 不进行分组操作（GROUP BY）
            Tuple t = tupleMap.get(NO_GROUP_BY);
            IntField field = (IntField) t.getField(1);
            t.setField(0, new IntField(field.getValue() + 1));
            tupleMap.put(NO_GROUP_BY, t);
        } else {
            // 进行分组操作（GROUP BY）
            Field field = tuple.getField(groupField); // 用于分组的字段
            if (!tupleMap.containsKey(field)) {
                Tuple t = new Tuple(this.tupleDesc);
                t.setField(0, field);
                t.setField(1, new IntField(1)); // 新字段，数量为1
                tupleMap.put(field, t);
            } else {
                Tuple t = tupleMap.get(NO_GROUP_BY);
                IntField intField = (IntField) t.getField(1);
                t.setField(0, new IntField(intField.getValue() + 1));
                tupleMap.put(NO_GROUP_BY, t);
            }
        }
    }

    /**
     * 创建一个迭代器用于遍历聚合分组的结果
     * @return 返回一个OpIterator
     * 如果进行分组，所遍历的元组格式为：(groupVal, aggregateVal)
     * 若未进行分组，元组格式为：(aggregateVal)
     * aggregateVal取决于构造函数中所确定的进行聚合的类型
     */
    @Override
    public OpIterator iterator() {
        return new StrOpIterator(this);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // StrOpIterator为StrAggregator的内部类
    class StrOpIterator implements OpIterator {

        private StrAggregator strAggregator;
        private Iterator<Tuple> iterator;

        public StrOpIterator(StrAggregator strAggregator) {
            this.strAggregator = strAggregator;
            iterator = null;
        }

        @Override
        public void open() throws DbException {
            iterator = strAggregator.tupleMap.values().iterator();
        }

        @Override
        public void close() {
            iterator = null;
        }

        @Override
        public boolean hasNext() throws DbException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException {
            iterator = strAggregator.tupleMap.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return strAggregator.tupleDesc;
        }
    }
}

