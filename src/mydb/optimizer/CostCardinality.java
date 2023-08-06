package mydb.optimizer;

import java.util.List;

/**
 * 用于表示一个查询计划的成本（cost）和基数（cardinality）
 */
public class CostCardinality {

    /**
     * 最优查询计划的成本
     */
    public double cost;

    /**
     * 最优查询计划的基数
     */
    public int cardinality;

    /**
     * 最优的查询计划
     */
    public List<LogicalJoinNode> plan;
}
