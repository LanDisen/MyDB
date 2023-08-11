package mydb.optimizer;

import mydb.optimizer.LogicalJoinNode;

import java.util.*;

/**
 * PlanCache用于存储一个查询计划中暂时最好的JOIN连接顺序
 */
public class PlanCache {

    /**
     * 最好的JOIN连接顺序
     */
    final Map<Set<LogicalJoinNode>, List<LogicalJoinNode>> bestOrders = new HashMap<>();

    final Map<Set<LogicalJoinNode>, Double> bestCosts = new HashMap<>();

    final Map<Set<LogicalJoinNode>, Integer> bestCardinalities = new HashMap<>();

    /**
     * 对一个JOIN集合，添加其JOIN顺序、成本和基数到PlanCache中
     * @param joinSet JOIN操作的集合
     * @param cost 该查询计划估计的成本
     * @param cardinality 该查询计划估计的基数
     * @param joinOrder JOIN操作的顺序
     */
    public void addPlan(Set<LogicalJoinNode> joinSet,
                        double cost, int cardinality,
                        List<LogicalJoinNode> joinOrder) {
        bestOrders.put(joinSet, joinOrder);
        bestCosts.put(joinSet, cost);
        bestCardinalities.put(joinSet, cardinality);
    }

    /**
     * 获取该查询计划中最佳JOIN顺序
     */
    public List<LogicalJoinNode> getOrder(Set<LogicalJoinNode> joinSet) {
        return bestOrders.get(joinSet);
    }

    /**
     * 获取该查询计划中最佳JOIN顺序对应的成本
     */
    public double getCost(Set<LogicalJoinNode> joinSet) {
        return bestCosts.get(joinSet);
    }

    /**
     * 获取该查询计划中最佳JOIN顺序对应的基数
     */
    public int getCardinality(Set<LogicalJoinNode> joinSet) {
        return bestCardinalities.get(joinSet);
    }
}
