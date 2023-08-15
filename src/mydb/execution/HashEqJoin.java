package mydb.execution;

import mydb.common.DbException;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionException;

import java.io.Serial;
import java.util.*;

/**
 * 基于哈希表对两个表进行等值连接
 */
public class HashEqJoin extends Operator {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 连接谓词
     */
    private final JoinPredicate joinPredicate;

    /**
     * 进行连接的两个表（左表和右表）
     */
    private OpIterator child1, child2;

    /**
     * 连接合并后的TupleDesc
     */
    private final TupleDesc mergedTupleDesc;

    transient private Tuple tuple1 = null;
    transient private Tuple tuple2 = null;

    /**
     * HashEqJoin构造函数
     * @param joinPredicate 连接谓词
     * @param child1 左表的操作符迭代器
     * @param child2 右表的操作符迭代器
     */
    public HashEqJoin(JoinPredicate joinPredicate, OpIterator child1, OpIterator child2) {
        this.joinPredicate = joinPredicate;
        this.child1 = child1;
        this.child2 = child2;
        this.mergedTupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return this.joinPredicate;
    }

    public String getLeftJoinFieldName() {
        return child1.getTupleDesc().getFieldName(joinPredicate.getFieldIndex1());
    }

    public String getRightJoinFieldName() {
        return child2.getTupleDesc().getFieldName(joinPredicate.getFieldIndex2());
    }

    /**
     * Key：字段（Field）；Value：元组列表
     */
    final Map<Object, List<Tuple>> map = new HashMap<>();

    public final static int MAP_SIZE = 19999; // 哈希表最大元素个数

    /**
     * 加载哈希表
     * @return 如果map不为空则返回true（成功加载了该map），否则返回false
     */
    private boolean loadMap() throws DbException, TransactionException {
        int cnt = 0;
        map.clear();
        while (child1.hasNext()) {
            tuple1 = child1.next();
            // 如果map中没有该key则创建一个List实例，将对应的key映射到该List
            List<Tuple> list = map.computeIfAbsent(
                    tuple1.getField(joinPredicate.getFieldIndex1()),
                    key -> new ArrayList<>());
            list.add(tuple1);
            // map已满
            if (cnt++ == MAP_SIZE) {
                return true;
            }
        }
        return cnt > 0;
    }

    public void open() throws DbException, NoSuchElementException, TransactionException {
        child1.open();
        child2.open();
        loadMap();
        super.open();
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
        this.tuple1 = null;
        this.tuple2 = null;
        this.tupleIterator = null;
        this.map.clear();
    }


    @Override
    public void rewind() throws DbException, TransactionException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * 左表（外表）的元组迭代器
     */
    transient Iterator<Tuple> tupleIterator = null;

    /**
     * 例：tuple1={1,2,3}，tuple2={1,5,6}，返回的元组为：{1,2,3,1,5,6}
     * @return 返回下一个由JOIN生成的元组（Tuple），如果没有更多元组则返回null
     */
//    private Tuple processList() {
//        if (!outerTupleIterator.hasNext()) {
//            return null;
//        }
//        tuple1 = outerTupleIterator.next();
//        int leftFieldsNum = tuple1.getTupleDesc().getFieldsNum();
//        int rightFieldsNum = tuple2.getTupleDesc().getFieldsNum();
//        // 接收连接后的新元组
//        Tuple tuple = new Tuple(this.mergedTupleDesc);
//        for (int i=0; i<leftFieldsNum; i++) {
//            tuple.setField(i, tuple1.getField(i));
//        }
//        for (int i=0; i<rightFieldsNum; i++) {
//            tuple.setField(i + leftFieldsNum, tuple2.getField(i));
//        }
//        return tuple; // 完成JOIN的元组
//    }

    /**
     * 例：tuple1={1,2,3}，tuple2={1,5,6}，返回的元组为：{1,2,3,1,5,6}
     * @return 返回下一个由JOIN生成的元组（Tuple），如果没有更多元组则返回null
     */
    @Override
    protected Tuple fetchNext() throws DbException, NoSuchElementException, TransactionException {
        if (tupleIterator != null && tupleIterator.hasNext()) {
            tuple1 = tupleIterator.next();
            int leftFieldsNum = tuple1.getTupleDesc().getFieldsNum();
            int rightFieldsNum = tuple2.getTupleDesc().getFieldsNum();
            // 接收连接后的新元组
            Tuple tuple = new Tuple(this.mergedTupleDesc);
            // 合并两个元组
            for (int i=0; i<leftFieldsNum; i++) {
                tuple.setField(i, tuple1.getField(i));
            }
            for (int i=0; i<rightFieldsNum; i++) {
                tuple.setField(i + leftFieldsNum, tuple2.getField(i));
            }
            return tuple; // 返回JOIN后的元组
        }
        // 循环右表
        while (child2.hasNext()) {
            tuple2 = child2.next();
            // map的key加载的是tuple1的字段
            // 将tuple2的字段作为key用于判断两个元组是否有同一个字段，相同字段才进行JOIN
            List<Tuple> list = map.get(tuple2.getField(joinPredicate.getFieldIndex2()));
            if (list == null) {
                continue;
            }
            // 两个元组的字段相同，可以JOIN
            tupleIterator = list.iterator(); // 获取child1中的元组的元组列表
            tuple1 = tupleIterator.next();
            int leftFieldsNum = tuple1.getTupleDesc().getFieldsNum();
            int rightFieldsNum = tuple2.getTupleDesc().getFieldsNum();
            // 用于接收连接后的新元组
            Tuple tuple = new Tuple(this.mergedTupleDesc);
            // 合并两个元组
            for (int i=0; i<leftFieldsNum; i++) {
                tuple.setField(i, tuple1.getField(i));
            }
            for (int i=0; i<rightFieldsNum; i++) {
                tuple.setField(i + leftFieldsNum, tuple2.getField(i));
            }
            return tuple; // 返回JOIN后的元组
        }
        // 嵌套循环查询
        child2.rewind();
        // 重新加载哈希表
        if (loadMap()) {
            return fetchNext();
        }
        return null;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.mergedTupleDesc;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }
}
