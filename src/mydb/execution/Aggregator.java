package mydb.execution;

import mydb.storage.Tuple;
import mydb.storage.TupleIterator;

import java.io.Serializable;

/**
 * 聚合器接口，用于对元组列表进行聚合操作计算
 */
public interface Aggregator {

    int hasGrouper = -1;

    enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT;

        public static Op getOp(String str) {
            return getOp(Integer.parseInt(str));
        }

        public static Op getOp(int index) {
            return values()[index];
        }

        @Override
        public String toString() {
            if (this == MIN) {
                return "MIN";
            }
            if (this == MAX) {
                return "MAX";
            }
            if (this == AVG) {
                return "AVG";
            }
            if (this == SUM) {
                return "SUM";
            }
            if (this == COUNT) {
                return "COUNT";
            }
            throw new IllegalStateException("unknown aggregator");
        }
    }

    /**
     * 将一个新的元组（tuple）合并到需要聚合的group中
     * 如果还没有元组则要先创建一个新的group
     * @param tuple 需要进行聚合的元组
     */
    void mergeTupleIntoGroup(Tuple tuple);

    /**
     * 获得一个迭代器用于遍历分组聚合的结果
     */
    OpIterator iterator();
    
}
