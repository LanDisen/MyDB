package mydb.optimizer;

import mydb.execution.Predicate;

/**
 * 逻辑查询计划中的JOIN结点，用于连接两个表
 */
public class LogicalJoinNode {

    public String leftTableAlias;
    public String rightTableAlias;

    /**
     * 进行JOIN的字段名称，仅是fieldName，而不是tableAlias.fieldName的形式
     */
    public String leftTableFieldName;

    /**
     * 进行JOIN的字段名称，仅是fieldName，而不是tableAlias.fieldName的形式
     */
    public String rightTableFieldName;

    /**
     * 包括tableAlias的字段名（tableAlias.fieldName）
     */
    public String leftTableFieldCompleteName;

    /**
     * 包括tableAlias的字段名（tableAlias.fieldName）
     */
    public String rightTableFieldCompleteName;

    /**
     * 连接谓词条件
     */
    public Predicate.Op op;

    public LogicalJoinNode() {}

    public LogicalJoinNode(String leftTableAlias, String rightTableAlias,
                           String leftJoinField, String rightJoinField,
                           Predicate.Op op) {
        this.leftTableAlias = leftTableAlias;
        this.rightTableAlias = rightTableAlias;
        // 分隔字段名和字段类型
        String[] temps = leftJoinField.split("[.]");
        if (temps.length > 1) {
            this.leftTableFieldName = temps[temps.length - 1];
        } else {
            this.leftTableFieldName = leftJoinField;
        }
        temps = rightJoinField.split("[.]");
        if (temps.length > 1) {
            this.rightTableFieldName = temps[temps.length - 1];
        } else {
            this.rightTableFieldName = leftJoinField;
        }
        this.op = op;
        this.leftTableFieldCompleteName = leftTableAlias + "." + leftTableFieldName;
        this.rightTableFieldCompleteName = rightTableAlias + "." + rightTableFieldName;
    }

    /**
     * @return 返回一个新的进行了内外表交换的LogicalJoinNode
     */
    public LogicalJoinNode swapInnerOuter() {
        Predicate.Op newOp;
        switch (this.op) {
            case GREATER_THAN -> {
                newOp = Predicate.Op.LESS_THAN;
            }
            case GREATER_THAN_OR_EQ -> {
                newOp = Predicate.Op.LESS_THAN_OR_EQ;
            }
            case LESS_THAN -> {
                newOp = Predicate.Op.GREATER_THAN;
            }
            case LESS_THAN_OR_EQ -> {
                newOp = Predicate.Op.GREATER_THAN_OR_EQ;
            }
            default -> {
                newOp = this.op;
            }
        }
        return new LogicalJoinNode(rightTableAlias, leftTableAlias, rightTableFieldName, leftTableFieldName, newOp);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LogicalJoinNode)) {
            return false;
        }
        LogicalJoinNode join = (LogicalJoinNode) o;
        return (join.leftTableFieldName.equals(rightTableAlias) && join.rightTableAlias.equals(leftTableAlias)) ||
                (join.leftTableFieldName.equals(leftTableAlias) && join.rightTableAlias.equals(rightTableAlias));
    }

    @Override
    public String toString() {
        return leftTableAlias + ":" + rightTableAlias;
    }

    @Override
    public int hashCode() {
        return leftTableAlias.hashCode() + rightTableAlias.hashCode() +
                leftTableFieldName.hashCode() + rightTableFieldName.hashCode();
    }
}
