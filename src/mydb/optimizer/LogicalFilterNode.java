package mydb.optimizer;

import mydb.execution.Predicate;

/**
 * LogicalFilterNode用于表示一个查询计划中WHERE子句的过滤条件
 */
public class LogicalFilterNode {

    public final String tableAlias;

    /**
     * 过滤谓词条件
     */
    public final Predicate.Op op;

    /**
     * filter右侧用于比较的常数值
     */
    public final String constant;

    /**
     * 不包括tableAlias的字段名（fieldName
     * ）
     */
    public final String fieldName;

    /**
     * 包括tableAlias的字段名（tableAlias.fieldName）
     */
    public final String fieldCompleteName;

    public LogicalFilterNode(String tableAlias, String fieldName,
                             Predicate.Op op, String constant) {
        this.tableAlias = tableAlias;
        this.op = op;
        this.constant = constant;
        String[] temps = fieldName.split("[.]");
        if (temps.length > 1) {
            this.fieldName = temps[temps.length - 1];
        } else {
            this.fieldName = fieldName;
        }
        this.fieldCompleteName = tableAlias + "." + this.fieldName;
    }




}
