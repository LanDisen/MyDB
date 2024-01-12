package mydb.optimizer;

import mydb.ParsingException;
import mydb.common.Database;
import mydb.execution.*;

import java.util.*;


/**
 * JoinOptimizer用于选择最优的逻辑计划来执行JOIN操作
 */
public class JoinOptimizer {

    /**
     * 逻辑查询计划
     */
    final LogicalPlan logicalPlan;
    final List<LogicalJoinNode> joins;

    /**
     * JoinOptimizer构造函数
     * @param logicalPlan 逻辑查询计划
     * @param joins 连接操作列表
     */
    public JoinOptimizer(LogicalPlan logicalPlan, List<LogicalJoinNode> joins) {
        this.logicalPlan = logicalPlan;
        this.joins = joins;
    }

    /**
     * 给定join结点、直方图统计信息和左右子查询结果，返回最优的连接结果（选择内外表连接顺序）
     * @param joinNode 连接操作结点
     * @param plan1 joinNode的左孩子（子查询计划）
     * @param plan2 joinNode的右孩子（子查询计划）
     * @return joinResult 最优连接结果
     */
    public static OpIterator bestJoin(LogicalJoinNode joinNode, OpIterator plan1, OpIterator plan2)
            throws ParsingException {
        // 进行JOIN的字段索引
        int leftTableFieldIndex = 0, rightTableFieldIndex = 0;
        // 连接结果
        OpIterator joinResult = null;
        try {
            leftTableFieldIndex  = plan1.getTupleDesc().fieldNameToIndex(joinNode.leftTableFieldCompleteName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + joinNode.leftTableFieldCompleteName);
        }
        if (!(joinNode instanceof LogicalSubplanJoinNode)) {
            try {
                rightTableFieldIndex = plan2.getTupleDesc().fieldNameToIndex(joinNode.rightTableFieldCompleteName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field " + joinNode.leftTableFieldCompleteName);
            }
        }
        JoinPredicate joinPredicate = new JoinPredicate(leftTableFieldIndex, joinNode.op, rightTableFieldIndex);
        if (joinNode.op.equals(Predicate.Op.EQUALS)) {
            // 等值连接
            try {
                // 优先使用HashEqJoin，不存在会使用普通Join
                Class<?> c = Class.forName("mydb.execution.HashEqJoin");
                // 利用反射调用构造函数
                java.lang.reflect.Constructor<?> constructor = c.getConstructors()[0];
                joinResult = (OpIterator) constructor.newInstance(new Object[] {joinPredicate, plan1, plan2});
            } catch (Exception e) {
                joinResult = new Join(joinPredicate, plan1, plan2);
            }
        } else {
            joinResult = new Join(joinPredicate, plan1, plan2);
        }
        return joinResult;
    }

    /**
     * 估计JOIN的成本（IO cost + CPU cost，谓词比较成本为1）
     * @param joinNode JOIN在逻辑查询计划中的结点
     * @param cardinality1 左孩子的基数
     * @param cardinality2 右孩子的基数
     * @param cost1 对左孩子扫描的成本
     * @param cost2 对右孩子扫描的成本
     * @return 返回这次查询估计的成本
     */
    public double estimateJoinCost(LogicalJoinNode joinNode,
                                   int cardinality1, int cardinality2,
                                   double cost1, double cost2) {
        if (joinNode instanceof LogicalSubplanJoinNode) {
            // 估计子查询的成本
            return cardinality1 + cost1 + cost2;
        } else {
            // leftTable JOIN rightTable
            // 连接成本为：scanCost(leftTable) + tupleTotalNum(leftTable) * scanCost(rightTable) // IO成本
            //           + tupleTotalNum(leftTable) * tupleTotalNum(rightTable) // CPU成本
            return cost1 + cardinality1 * cost2 + cardinality1 * cardinality2;
        }
    }

    /**
     * 估计JOIN的基数，即JOIN产生的元组数量
     * @param joinNode 在查询计划中的JOIN结点
     * @param cardinality1 左表基数
     * @param cardinality2 右表基数
     * @param isLeftPrimaryKey 左表是否有主键
     * @param isRightPrimaryKey 右表是否有主键
     * @param stats 统计信息Map，表名（而不是别名）作为Key
     * @return 返回这次JOIN操作估计的基数
     */
    public int estimateJoinCardinality(LogicalJoinNode joinNode,
                                       int cardinality1, int cardinality2,
                                       boolean isLeftPrimaryKey, boolean isRightPrimaryKey,
                                       Map<String, TableStats> stats) {
        if (joinNode instanceof LogicalSubplanJoinNode) {
            // 返回这次子查询的基数
            return cardinality1;
        } else {
            return estimateTableJoinCardinality(joinNode.op,
                    joinNode.leftTableAlias, joinNode.rightTableAlias,
                    joinNode.leftTableFieldName, joinNode.rightTableFieldName,
                    cardinality1, cardinality2,
                    isLeftPrimaryKey, isRightPrimaryKey,
                    stats, logicalPlan.getTableAliasToIdMapping());
        }
    }

    /**
     * 估计两个表JOIN的基数
     */
    public static int estimateTableJoinCardinality(
            Predicate.Op joinOp,
            String leftTableAlias, String rightTableAlias,
            String leftFieldName, String rightFieldName,
            int cardinality1, int cardinality2,
            boolean isLeftPrimaryKey, boolean isRightPrimaryKey,
            Map<String, TableStats> stats,
            Map<String, Integer> tableAliasToIdMap) {
        int cardinality = 1;
        switch (joinOp) {
            case EQUALS -> {
                if (isLeftPrimaryKey && !isRightPrimaryKey) {
                    // 右表操作的字段不是主键
                    cardinality = cardinality2;
                } else if (!isLeftPrimaryKey && isRightPrimaryKey) {
                    // 左表操作的字段不是主键
                    cardinality = cardinality1;
                } else if (isLeftPrimaryKey && isRightPrimaryKey) {
                    // 两个字段都是主键
                    cardinality = Math.min(cardinality1, cardinality2);
                } else {
                    // 两个字段都不是主键
                    cardinality = Math.max(cardinality1, cardinality2);
                }
            }
            case NOT_EQUALS -> {
                if (isLeftPrimaryKey && !isRightPrimaryKey) {
                    // 右表操作的字段不是主键
                    cardinality = cardinality1 * cardinality2 - cardinality2;
                } else if (!isLeftPrimaryKey && isRightPrimaryKey) {
                    // 左表操作的字段不是主键
                    cardinality = cardinality1 * cardinality2 - cardinality1;
                } else if (isLeftPrimaryKey && isRightPrimaryKey) {
                    // 两个字段都是主键
                    cardinality =cardinality1 * cardinality2 -
                            Math.min(cardinality1, cardinality2);
                } else {
                    // 两个字段都不是主键
                    cardinality = cardinality1 * cardinality2 -
                            Math.max(cardinality1, cardinality2);
                }

            }
            default -> {
                // 按照范围查询计算基数
                cardinality = (int) (0.3 * cardinality1 * cardinality2);
            }
        }
        return cardinality <= 0 ? 1 : cardinality;
    }

    /**
     * 辅助函数，给定列表及长度，枚举指定长度的子集（subsets）
     * 例：list={1,2,3}，size=2，返回{{1,2},{1,3},{2,3}}
     * @param list 需要枚举的列表
     * @param size 枚举长度大小
     * @return 返回所有大小为size的子集的集合
     */
    public <T> Set<Set<T>> enumerateSubsets(List<T> list, int size) {
        Set<Set<T>> sets = new HashSet<>();
        sets.add(new HashSet<>());
        for (int i=0; i<size; i++) {
            Set<Set<T>> newSets = new HashSet<>();
            for (Set<T> set: sets) {
                // 遍历列表的每个元素
                for (T t: list) {
                    // 将set复制到newSet
                    Set<T> newSet = new HashSet<>(set);
                    // 若t不在newSet中则添加成功
                    if (newSet.add(t)) {
                        newSets.add(newSet);
                    }
                }
            }
            sets = newSets;
        }
        return sets;
    }

    /**
     * 计算高效的连接顺序
     * @param stats 统计信息Map（Key：表名；Value：表的统计信息）
     * @param filterSelectivities 过滤操作的选择性
     * @param explain 可视化解释查询计划（true）或只是执行代码（false）
     * @return 返回JOIN结点的列表，其保存了应该执行的JOIN顺序
     */
    public List<LogicalJoinNode> orderJoins(
            Map<String, TableStats> stats,
            Map<String, Double> filterSelectivities,
            boolean explain) throws ParsingException {
        PlanCache planCache = new PlanCache();
        CostCardinality bestCordCardinality = new CostCardinality();
        int size = this.joins.size(); // 连接结点的数目
        for (int i=1; i<=size; i++) {
            // 获得给定size的所有子集
            Set<Set<LogicalJoinNode>> subsets = enumerateSubsets(joins, i);
            for (Set<LogicalJoinNode> subset: subsets) {
                double bestCost = Double.MAX_VALUE; // 存储至今最少的成本
                for (LogicalJoinNode join: subset) {
                    // 计算往subset加入join后的成本和基数
                    CostCardinality costCardinality = computeCostCardinality(
                            stats, filterSelectivities, join, subset, bestCost, planCache);
                    if (costCardinality == null) {
                        // 未得到最好的成本和基数
                        continue;
                    }
                    bestCost = costCardinality.cost;
                    bestCordCardinality = costCardinality;
                }
                if (bestCost != Double.MAX_VALUE) {
                    // 缓存当前最佳查询计划
                    planCache.addPlan(subset, bestCordCardinality.cost,
                            bestCordCardinality.cardinality, bestCordCardinality.plan);
                }
            }
        }
        if (explain) {
            printJoins(bestCordCardinality.plan, planCache, stats, filterSelectivities);
        }
        // 返回最佳的JOIN顺序
        return bestCordCardinality.plan;
    }

    // ==================private方法=====================

    /**
     * 计算将一个JOIN（joinToRemove）加入到joinSet（joinSet需包含joinToRemove）的成本和基数。
     * 一个JOIN的成本和基数的计算结果为从集合中删除该JOIN后变化的成本和基数的值。
     * @param stats 统计信息，Key为表名（而不是别名）
     * @param filterSelectivities 过滤器选择性
     * @param joinToRemove 考虑计算成本基数的JOIN操作
     * @param joinSet JOIN集合（需要包含joinToRemove）
     * @param bestCost 至今最佳查询成本
     * @param planCache 缓存之前最好JOIN顺序的查询计划
     * @return 返回CostCardinality，其包括了最优计划以及对应的成本和基数
     */
    private CostCardinality computeCostCardinality(
            Map<String, TableStats> stats,
            Map<String, Double> filterSelectivities,
            LogicalJoinNode joinToRemove,
            Set<LogicalJoinNode> joinSet,
            double bestCost,
            PlanCache planCache) throws ParsingException {
        LogicalJoinNode join = joinToRemove;
        // 未删除该join时的连接顺序，可用于得到对应的成本和基数
        List<LogicalJoinNode> prevBestOrder;
        // 该逻辑计划中没有该连接的表名会抛出异常，
        if (this.logicalPlan.getTableId(join.leftTableAlias) == null) {
            throw new ParsingException("Unknown table " + join.leftTableAlias);
        }
        if (this.logicalPlan.getTableId(join.rightTableAlias) == null) {
            throw new ParsingException("Unknown table " + join.rightTableAlias);
        }
        // join（joinToRemove）的左右孩子信息
        String leftTableName = Database.getCatalog().getTableName(logicalPlan.getTableId(join.leftTableAlias));
        String rightTableName = Database.getCatalog().getTableName(logicalPlan.getTableId(join.rightTableAlias));
        String leftTableAlias = join.leftTableAlias;
        String rightTableAlias = join.rightTableAlias;
        // 创建一个与joinSet相同的副本
        Set<LogicalJoinNode> newJoinSet = new HashSet<>(joinSet);
        // 将需要删除的JOIN从集合中删去
        newJoinSet.remove(join);

        double leftTableCost, rightTableCost;
        int leftTableCardinality, rightTableCardinality;
        boolean leftPrimaryKey, rightPrimaryKey;

        if (newJoinSet.isEmpty()) {
            // joinSet只有joinToRemove，直接计算joinToRemove的成本和基数
            prevBestOrder = new ArrayList<>();
            leftTableCost = stats.get(leftTableName).estimateScanCost();
            rightTableCost = stats.get(rightTableName).estimateScanCost();
            leftTableCardinality = stats.get(leftTableName).estimateTableCardinality(
                    filterSelectivities.get(join.leftTableAlias));
            rightTableCardinality = stats.get(rightTableName).estimateTableCardinality(
                    filterSelectivities.get(join.rightTableAlias));
            leftPrimaryKey = isPrimaryKey(join.leftTableAlias, join.leftTableFieldName);
            rightPrimaryKey = isPrimaryKey(join.rightTableAlias, join.rightTableFieldName);
        } else {
            // newJoinSet不为空
            prevBestOrder = planCache.getOrder(newJoinSet);
            // 可能未在planCache保存最佳的顺序
            if (prevBestOrder == null) {
                return null;
            }
            // 不考虑joinToRemove的连接成本和基数
            double preBestCost = planCache.getCost(newJoinSet);
            int prevBestCardinality = planCache.getCardinality(newJoinSet);

            if (doesJoin(prevBestOrder, leftTableAlias)) {
                // 左表已在最佳的连接顺序中
                leftTableCost = preBestCost;
                leftTableCardinality = prevBestCardinality;
                leftPrimaryKey = hasPrimaryKey(prevBestOrder);

                rightTableCost = join.rightTableAlias == null ? 0 :
                        stats.get(leftTableName).estimateScanCost();
                rightTableCardinality = join.rightTableAlias == null ? 0 :
                        stats.get(leftTableName).estimateTableCardinality(filterSelectivities.get(rightTableAlias));
                rightPrimaryKey = join.rightTableAlias != null &&
                        isPrimaryKey(join.rightTableAlias, join.rightTableFieldName);
            } else if (doesJoin(prevBestOrder, join.rightTableAlias)) {
                // 右表已在最佳的连接顺序中
                rightTableCost = preBestCost;
                rightTableCardinality = prevBestCardinality;
                rightPrimaryKey = hasPrimaryKey(prevBestOrder);

                leftTableCost = stats.get(leftTableName).estimateScanCost();
                leftTableCardinality = stats.get(leftTableName).estimateTableCardinality(
                        filterSelectivities.get(leftTableAlias));
                leftPrimaryKey = isPrimaryKey(join.leftTableAlias, join.leftTableFieldName);
            } else {
                // 两个表都不在最佳连接顺序中，不考虑其成本和基数
                return null;
            }
        }
        // 选择嵌套连接顺序
        double cost1 = estimateJoinCost(join,
                leftTableCardinality, rightTableCardinality,
                leftTableCost, rightTableCost);
        // 左右表交换后的JOIN操作
        LogicalJoinNode join2 = join.swapInnerOuter();
        double cost2 = estimateJoinCost(join2,
                rightTableCardinality, leftTableCardinality,
                rightTableCost, leftTableCost);
        if (cost2 < cost1) {
            // 内外表交换后更高效
            join = join2;
            cost1 = cost2;
            boolean temp = rightPrimaryKey;
            rightPrimaryKey = leftPrimaryKey;
            leftPrimaryKey = temp;
        }
        if (cost1 >= bestCost) {
            return null;
        }
        CostCardinality cc = new CostCardinality();
        cc.cardinality = estimateJoinCardinality(join,
                leftTableCardinality, rightTableCardinality,
                leftPrimaryKey, rightPrimaryKey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBestOrder);
        cc.plan.add(join);
        return cc;
    }

    /**
     * @return 若指定的表在JOIN列表中则返回true，否则返回false
     */
    private boolean doesJoin(List<LogicalJoinNode> joins, String tableAlias) {
        // 遍历JOIN结点判断有无对该表操作的JOIN结点
        for (LogicalJoinNode joinNode: joins) {
            if (joinNode.leftTableAlias.equals(tableAlias)) {
                return true;
            }
            if (joinNode.rightTableAlias != null && joinNode.rightTableAlias.equals(tableAlias)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断字段是否为指定表的主键
     * @param tableAlias 表名（或别名）
     * @param fieldName 字段名
     */
    private boolean isPrimaryKey(String tableAlias, String fieldName) {
        int tableId = logicalPlan.getTableId(tableAlias);
        String primaryKeyName = Database.getCatalog().getPrimaryKey(tableId);
        return fieldName.equals(primaryKeyName);
    }

    /**
     * @return 若有一个JOIN结点对主键操作则返回true，否则返回false
     */
    private boolean hasPrimaryKey(List<LogicalJoinNode> joins) {
        for (LogicalJoinNode join: joins) {
            if (isPrimaryKey(join.leftTableAlias, join.leftTableFieldName)) {
                return true;
            } else if (join.rightTableAlias != null &&
                    isPrimaryKey(join.rightTableAlias, join.rightTableFieldName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 辅助函数，用于在Swing窗口中可视化joins的语法分析树
     */
    private void printJoins(List<LogicalJoinNode> joins,
                            PlanCache planCache,
                            Map<String, TableStats> stats,
                            Map<String, Double> selectivities) {
        // TODO
    }
}
