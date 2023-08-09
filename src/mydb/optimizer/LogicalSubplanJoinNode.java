package mydb.optimizer;

import mydb.execution.OpIterator;
import mydb.execution.Predicate;

/**
 * 子查询连接结点，用于连接（JOIN）一个字段和一个子查询的结果并将该结点添加到逻辑查询计划中
 */
public class LogicalSubplanJoinNode extends LogicalJoinNode {

    final OpIterator subplan;

    public LogicalSubplanJoinNode(String tableAlias, String joinField, OpIterator subplan, Predicate.Op op) {
        this.leftTableAlias = tableAlias;
        String[] temps = joinField.split("[.]");
        if (temps.length > 1) {
            this.leftTableFieldName = temps[temps.length - 1]; // 最后一个为字段名
        } else {
            this.leftTableFieldName = joinField;
        }
        this.subplan = subplan;
        this.op = op;
    }

    public LogicalSubplanJoinNode swapInnerOuter() {
        return new LogicalSubplanJoinNode(leftTableAlias, leftTableFieldName, subplan, op);
    }

    @Override
    public int hashCode() {
        return leftTableAlias.hashCode() + leftTableFieldName.hashCode() + subplan.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LogicalJoinNode)) {
            return false;
        }
        LogicalJoinNode joinNode = (LogicalJoinNode) o;
        return joinNode.leftTableFieldName.equals(leftTableAlias) &&
                joinNode.leftTableFieldName.equals(leftTableFieldName) &&
                ((LogicalSubplanJoinNode)o).subplan.equals(this.subplan);
    }
}
