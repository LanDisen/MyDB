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

public class IntAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUP_BY = new IntField(-1);

    private int groupField;
    private Type groupFieldType;
    private int aggregateField;
    private Op op;

    private TupleDesc tupleDesc;
    private Map<Field, Tuple> aggregateMap; // 聚合计算后的结果

    // 无GROUP BY分组
    private int count;
    private int sum;
    // 有GROUP BY分组
    private Map<Field, Integer> countMap; // COUNT操作后的分组结果
    private Map<Field, Integer> sumMap; // SUM操作后的结果


    /**
     * IntAggregator构造函数
     * @param groupField 进行GROUP BY分组的字段索引，若不进行分组则为-1
     * @param groupFieldType 进行GROUP BY分组的字段类型，若不进行分组则为null
     * @param aggregateField 进行聚合操作的字段索引
     * @param op 聚合操作符（Aggregate.Op）
     */
    public IntAggregator(int groupField, Type groupFieldType, int aggregateField, Aggregator.Op op) {
        this.groupField = groupField;
        this.groupFieldType = groupFieldType;
        this.aggregateField = aggregateField;
        this.op = op;
        this.aggregateMap = new ConcurrentHashMap<>();
        if (groupField == -1) { // 不进行GROUP BY分组操作
            this.tupleDesc = new TupleDesc(
                    new Type[] {Type.INT_TYPE},
                    new String[] {"aggregateValue"}
            );
            Tuple tuple = new Tuple(this.tupleDesc);
            this.aggregateMap.put(NO_GROUP_BY, tuple);
        } else {
            this.tupleDesc = new TupleDesc(
                    new Type[] {groupFieldType, Type.INT_TYPE},
                    new String[] {"groupValue", "aggregateValue"}
            );
        }
        if (groupField == -1 && op.equals(Op.AVG)) {
            this.count = 0;
            this.sum = 0;
        } else {
            this.countMap = new ConcurrentHashMap<>();
            this.sumMap = new ConcurrentHashMap<>();
        }
    }

    /**
     * 将新的元组合并到聚合操作中，按照构造函数的指示进行分组
     * @param tuple 需要进行聚合的元组
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tuple) {
        // 准备进行聚合操作的字段（待合并）
        IntField aggField = (IntField) tuple.getField(aggregateField);
        if (aggField == null) {
            return;
        }
        if (groupField == -1) { // 不进行分组操作
            // 不分组（GROUP BY）则聚合结果只有一个
            Tuple tempTuple = aggregateMap.get(NO_GROUP_BY);
            IntField field = (IntField) tempTuple.getField(0);
            if (field == null) {
                // 聚合结果中没有该字段，添加第一个字段元素
                if (op.equals(Op.COUNT)) {
                    tempTuple.setField(0, new IntField(1));
                } else if (op.equals(Op.AVG)) {
                    this.count += 1;
                    this.sum = aggField.getValue();
                    tempTuple.setField(0, aggField);
                } else {
                    tempTuple.setField(0, aggField);
                }
                aggregateMap.put(NO_GROUP_BY, tempTuple);
                return;
            }
            switch (op) {
                case MIN -> {
                    if (aggField.compare(Predicate.Op.LESS_THAN, field)) {
                        tempTuple.setField(0, aggField);
                        aggregateMap.put(NO_GROUP_BY, tempTuple);
                    }
                }
                case MAX -> {
                    if (aggField.compare(Predicate.Op.GREATER_THAN, field)) {
                        tempTuple.setField(0, aggField);
                        aggregateMap.put(NO_GROUP_BY, tempTuple);
                    }
                }
                case AVG -> {
                    count++;
                    sum += aggField.getValue();
                    IntField avgField = new IntField(sum / count);
                    tempTuple.setField(0, avgField);
                    aggregateMap.put(NO_GROUP_BY, tempTuple);
                }
                case COUNT -> {
                    IntField countField = new IntField(field.getValue() + 1);
                    tempTuple.setField(0, countField);
                    aggregateMap.put(NO_GROUP_BY, tempTuple);
                }
                case SUM -> {
                    IntField sumField = new IntField(field.getValue() + aggField.getValue());
                    tempTuple.setField(0, sumField);
                    aggregateMap.put(NO_GROUP_BY, tempTuple);
                }
                default -> {
                    // unknown Aggregate.Op type
                }
            }
        } else {
            // 进行了分组操作（GROUP BY）
            Field gField = tuple.getField(groupField);
            if (!aggregateMap.containsKey(gField)) {
                // 聚合结果无该分组字段元素，此时添加第一个该字段元素
                Tuple t = new Tuple(this.tupleDesc);
                t.setField(0, gField);
                if (op.equals(Op.COUNT)) {
                    // 目前COUNT数量为1
                    t.setField(1, new IntField(1));
                } else if (op.equals(Op.AVG)) {
                    countMap.put(gField, countMap.getOrDefault(gField, 0) + 1);
                    sumMap.put(gField, sumMap.getOrDefault(gField, 0) + aggField.getValue());
                    t.setField(1, aggField);
                }
                aggregateMap.put(gField, t);
                return;
            }
            Tuple t = aggregateMap.get(gField);
            IntField field = (IntField) t.getField(1);
            switch (op) {
                case MIN -> {
                    if (aggField.compare(Predicate.Op.LESS_THAN, field)) {
                        t.setField(1, aggField);
                        aggregateMap.put(gField, t);
                    }
                }
                case MAX -> {
                    if (aggField.compare(Predicate.Op.GREATER_THAN, field)) {
                        t.setField(1, aggField);
                        aggregateMap.put(gField, t);
                    }
                }
                case AVG -> {
                    countMap.put(gField, countMap.getOrDefault(gField, 0) + 1);
                    sumMap.put(gField, sumMap.getOrDefault(gField, 0) + 1);
                    IntField avgField = new IntField(sumMap.get(gField) / countMap.get(gField));
                    t.setField(1, aggField);
                    aggregateMap.put(gField, t);
                }
                case COUNT -> {
                    IntField countField = new IntField(field.getValue() + 1);
                    t.setField(1, countField);
                    aggregateMap.put(gField, t);
                }
                case SUM -> {
                    IntField sumField = new IntField(field.getValue() + aggField.getValue());
                    t.setField(1, sumField);
                    aggregateMap.put(gField, t);
                }
                default -> {
                    // unknown Aggregate.Op type
                }
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
        return new IntOpIterator(this);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // IntOpIterator为IntAggregator的内部类
    class IntOpIterator implements OpIterator {

        private Iterator<Tuple> iterator;
        private IntAggregator intAggregator;

        public IntOpIterator(IntAggregator intAggregator) {
            this.intAggregator = intAggregator;
            this.iterator = null;
        }

        @Override
        public void open() throws DbException {
            this.iterator = intAggregator.aggregateMap.values().iterator();
        }

        @Override
        public void close() {
            this.iterator = null;
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
            this.iterator = intAggregator.aggregateMap.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return intAggregator.tupleDesc;
        }
    }
}
