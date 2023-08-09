package mydb.optimizer;

import mydb.execution.OpIterator;
import mydb.execution.Predicate;

/**
 * �Ӳ�ѯ���ӽ�㣬�������ӣ�JOIN��һ���ֶκ�һ���Ӳ�ѯ�Ľ�������ý����ӵ��߼���ѯ�ƻ���
 */
public class LogicalSubplanJoinNode extends LogicalJoinNode {

    final OpIterator subplan;

    public LogicalSubplanJoinNode(String tableAlias, String joinField, OpIterator subplan, Predicate.Op op) {
        this.leftTableAlias = tableAlias;
        String[] temps = joinField.split("[.]");
        if (temps.length > 1) {
            this.leftTableFieldName = temps[temps.length - 1]; // ���һ��Ϊ�ֶ���
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
