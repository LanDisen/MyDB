package mydb.optimizer;

import mydb.common.Database;
import mydb.execution.*;

import javax.xml.crypto.Data;
import java.util.Map;

/**
 * 辅助类，用于估计一棵SQL树的基数（cardinality）
 */
public class OperatorCardinality {

    /**
     * @param op 操作符（Filter、Aggregate、Join等）
     * @param tableAliasToId 表的别名与表ID的映射
     * @param tableStats 表的统计信息
     * @return 返回连接是否操作了主键字段，用于递归更新操作符基数
     */
    public static boolean updateOperatorCardinality(Operator op,
                                                    Map<String, Integer> tableAliasToId,
                                                    Map<String, TableStats> tableStats) {
        if (op instanceof Filter) {
            return updateFilterCardinality((Filter) op, tableAliasToId, tableStats);
        } else if (op instanceof Join) {
            // 连接操作
            return updateJoinCardinality((Join) op, tableAliasToId, tableStats);
        }  else if (op instanceof HashEqJoin) {
            // Hash等值连接
            return updateHashEqJoinCardinality((HashEqJoin) op, tableAliasToId, tableStats);
        } else if (op instanceof Aggregate) {
            return updateAggregateCardinality((Aggregate) op, tableAliasToId, tableStats);
        } else {
            OpIterator[] children = op.getChildren(); // 子节点
            boolean hasJoinPrimaryKey = false;
            int childCardinality = 1; // 子节点操作符的基数
            if (children.length > 0 && children[0] != null) {
                if (children[0] instanceof Operator) {
                    hasJoinPrimaryKey = updateOperatorCardinality((Operator) children[0], tableAliasToId, tableStats);
                    // 估计子节点操作符的基数
                    childCardinality = ((Operator) children[0]).getEstimatedCardinality();
                } else if (children[0] instanceof SeqScan) {
                    // 扫描操作符的选择性为1.0（全表扫描）
                    childCardinality = tableStats.get(((SeqScan) children[0]).getTableName()).estimateTableCardinality(1.0);
                }
            }
            op.setEstimatedCardinality(childCardinality);
            return hasJoinPrimaryKey;
        }
    }

    private static boolean updateFilterCardinality(Filter filter,
                                                   Map<String, Integer> tableAliasToId,
                                                   Map<String, TableStats> tableStats) {
        OpIterator child = filter.getChildren()[0];
        Predicate predicate = filter.getPredicate();
        // tableAlias.fieldName
        String[] temp = child.getTupleDesc().getFieldName(predicate.getFieldIndex()).split("[.]");
        String tableAlias = temp[0];
        String fieldName = temp[1];
        Integer tableId = tableAliasToId.get(tableAlias);
        double selectivity = 1.0;
        // 存在该表
        if (tableId != null) {
            String tableName = Database.getCatalog().getTableName(tableId);
            int fieldIndex = Database.getCatalog().getTupleDesc(tableId).fieldNameToIndex(fieldName);
            // 估计过滤谓词在该表的选择度
            selectivity  = tableStats.get(tableName).estimateSelectivity(fieldIndex, predicate.getOp(), predicate.getOperand());
            if (child instanceof Operator) {
                Operator childOp = (Operator) child;
                boolean hasJoinPrimaryKey = updateOperatorCardinality(childOp, tableAliasToId, tableStats);
                filter.setEstimatedCardinality((int) (childOp.getEstimatedCardinality() * selectivity) + 1);
                return hasJoinPrimaryKey;
            } else if (child instanceof SeqScan) {
                String scanTableName = ((SeqScan) child).getTableName();
                int cardinality = (int) (tableStats.get(scanTableName).estimateTableCardinality(1.0) * selectivity) + 1;
                filter.setEstimatedCardinality(cardinality);
                return false; // 没有JOIN
            }
        }
        filter.setEstimatedCardinality(1);
        return false;
    }

    private static boolean updateJoinCardinality(Join join,
                                                 Map<String, Integer> tableAliasToId,
                                                 Map<String, TableStats> tableStats) {
        OpIterator[] children = join.getChildren();
        // JOIN结点的左右孩子
        OpIterator leftChild = children[0];
        OpIterator rightChild = children[1];
        // 操作符基数
        int leftChildCardinality = 1;
        int rightChildCardinality = 1;

        // tableAlias.fieldName
        String[] temp1 = join.getJoinField1Name().split("[.]");
        String leftTableAlias = temp1[0];
        String leftFieldName = temp1[1];
        String[] temp2 = join.getJoinField2Name().split("[.]");
        String rightTableAlias = temp2[0];
        String rightFieldName = temp2[1];

        int leftTableId = tableAliasToId.get(leftTableAlias);
        int rightTableId = tableAliasToId.get(rightTableAlias);
        // 判断左右孩子操作符是否有连接主键（连接字段是否是对应表的主键）
        boolean leftChildHasJoinPrimaryKey = Database.getCatalog().getPrimaryKey(leftTableId).equals(leftFieldName);
        boolean rightChildHasJoinPrimaryKey = Database.getCatalog().getPrimaryKey(rightTableId).equals(rightFieldName);

        if (leftChild instanceof Operator) {
            Operator leftChildOp = (Operator) leftChild;
            boolean hasPrimaryKey = updateOperatorCardinality(leftChildOp, tableAliasToId, tableStats);
            leftChildHasJoinPrimaryKey = hasPrimaryKey || leftChildHasJoinPrimaryKey;
            leftChildCardinality = leftChildOp.getEstimatedCardinality();
            leftChildCardinality = leftChildCardinality > 0 ? leftChildCardinality : 1;
        } else if (leftChild instanceof SeqScan) {
            // 全表扫描操作符
            leftChildCardinality = tableStats.get(((SeqScan) leftChild).getTableName()).estimateTableCardinality(1.0);
        }

        if (rightChild instanceof Operator) {
            Operator rightChildOp = (Operator) rightChild;
            boolean hasPrimaryKey = updateOperatorCardinality(rightChildOp, tableAliasToId, tableStats);
            rightChildHasJoinPrimaryKey = hasPrimaryKey || rightChildHasJoinPrimaryKey;
            rightChildCardinality = rightChildOp.getEstimatedCardinality();
            rightChildCardinality = rightChildCardinality > 0 ? rightChildCardinality : 1;
        } else if (rightChild instanceof SeqScan) {
            // 全表扫描操作符
            rightChildCardinality = tableStats.get(((SeqScan) rightChild).getTableName()).estimateTableCardinality(1.0);
        }
        // 估计两个表JOIN的基数
        int joinCardinality = JoinOptimizer.estimateTableJoinCardinality(
                join.getJoinPredicate().getOp(), leftTableAlias, rightTableAlias,
                leftFieldName, rightFieldName, leftChildCardinality, rightChildCardinality,
                leftChildHasJoinPrimaryKey, rightChildHasJoinPrimaryKey, tableStats, tableAliasToId
        );
        // 设置连接基数
        join.setEstimatedCardinality(joinCardinality);
        return leftChildHasJoinPrimaryKey || rightChildHasJoinPrimaryKey;
    }

    private static boolean updateHashEqJoinCardinality(HashEqJoin join,
                                            Map<String, Integer> tableAliasToId,
                                            Map<String, TableStats> tableStats) {
        OpIterator[] children = join.getChildren();
        // JOIN结点的左右孩子
        OpIterator leftChild = children[0];
        OpIterator rightChild = children[1];
        // 操作符基数
        int leftChildCardinality = 1;
        int rightChildCardinality = 1;

        // tableAlias.fieldName
        String[] temp1 = join.getLeftJoinFieldName().split("[.]");
        String leftTableAlias = temp1[0];
        String leftFieldName = temp1[1];
        String[] temp2 = join.getRightJoinFieldName().split("[.]");
        String rightTableAlias = temp2[0];
        String rightFieldName = temp2[1];

        int leftTableId = tableAliasToId.get(leftTableAlias);
        int rightTableId = tableAliasToId.get(rightTableAlias);
        // 判断左右孩子操作符是否有连接主键（连接字段是否是对应表的主键）
        boolean leftChildHasJoinPrimaryKey = Database.getCatalog().getPrimaryKey(leftTableId).equals(leftFieldName);
        boolean rightChildHasJoinPrimaryKey = Database.getCatalog().getPrimaryKey(rightTableId).equals(rightFieldName);

        if (leftChild instanceof Operator) {
            Operator leftChildOp = (Operator) leftChild;
            boolean hasPrimaryKey = updateOperatorCardinality(leftChildOp, tableAliasToId, tableStats);
            leftChildHasJoinPrimaryKey = hasPrimaryKey || leftChildHasJoinPrimaryKey;
            leftChildCardinality = leftChildOp.getEstimatedCardinality();
            leftChildCardinality = leftChildCardinality > 0 ? leftChildCardinality : 1;
        } else if (leftChild instanceof SeqScan) {
            // 全表扫描操作符
            leftChildCardinality = tableStats.get(((SeqScan) leftChild).getTableName()).estimateTableCardinality(1.0);
        }

        if (rightChild instanceof Operator) {
            Operator rightChildOp = (Operator) rightChild;
            boolean hasPrimaryKey = updateOperatorCardinality(rightChildOp, tableAliasToId, tableStats);
            rightChildHasJoinPrimaryKey = hasPrimaryKey || rightChildHasJoinPrimaryKey;
            rightChildCardinality = rightChildOp.getEstimatedCardinality();
            rightChildCardinality = rightChildCardinality > 0 ? rightChildCardinality : 1;
        } else if (rightChild instanceof SeqScan) {
            // 全表扫描操作符
            rightChildCardinality = tableStats.get(((SeqScan) rightChild).getTableName()).estimateTableCardinality(1.0);
        }
        // 估计两个表JOIN的基数
        int joinCardinality = JoinOptimizer.estimateTableJoinCardinality(
                join.getJoinPredicate().getOp(), leftTableAlias, rightTableAlias,
                leftFieldName, rightFieldName, leftChildCardinality, rightChildCardinality,
                leftChildHasJoinPrimaryKey, rightChildHasJoinPrimaryKey, tableStats, tableAliasToId
        );
        // 设置连接基数
        join.setEstimatedCardinality(joinCardinality);
        return leftChildHasJoinPrimaryKey || rightChildHasJoinPrimaryKey;
    }

    private static boolean updateAggregateCardinality(Aggregate aggregate,
                                                      Map<String, Integer> tableAliasToId,
                                                      Map<String, TableStats> tableStats) {
        OpIterator child = aggregate.getChildren()[0];
        int childCardinality = 1;
        boolean hasJoinPrimaryKey = false;
        if (child instanceof Operator) {
            Operator childOp = (Operator) child;
            hasJoinPrimaryKey = updateOperatorCardinality(childOp, tableAliasToId, tableStats);
            childCardinality = childOp.getEstimatedCardinality();
        }
        if (child instanceof SeqScan) {
            String scanTableName = ((SeqScan) child).getTableName();
            // SeqScan的选择度为1
            childCardinality = tableStats.get(scanTableName).estimateTableCardinality(1.0);
        }
        if (aggregate.getGroupField() == -1) {
            // 没有分组字段
            aggregate.setEstimatedCardinality(1);
            return hasJoinPrimaryKey;
        }
        // 存在分组字段
        String[] temp = aggregate.getGroupFieldName().split("[.]");
        String tableAlias = temp[0];
        String fieldName = temp[1];
        Integer tableId = tableAliasToId.get(tableAlias);

        double avgSelectivity = 1.0;
        if (tableId != null) {
            // 计算分组平均选择度
            String tableName = Database.getCatalog().getTableName(tableId);
            int fieldIndex = Database.getCatalog().getTupleDesc(tableId).fieldNameToIndex(fieldName); // 分组操作字段
            // 分组操作使用了EQUALS谓词
            avgSelectivity = tableStats.get(tableName).avgSelectivity(fieldIndex, Predicate.Op.EQUALS);
            int cardinality = (int) Math.min(childCardinality, 1.0 / avgSelectivity);
            aggregate.setEstimatedCardinality(cardinality);
            return hasJoinPrimaryKey;
        }
        aggregate.setEstimatedCardinality(childCardinality);
        return hasJoinPrimaryKey;
    }
}
