package mydb.optimizer;


/**
 * ???
 * LogicalSelectListNode用于一个逻辑查询计划中选择（SELECT）列表的子句
 */
public class LogicalSelectListNode {
    /**
     * 字段名，可以带有表名（可选）
     */
    public final String fieldName;

    /**
     * 对指定字段的聚合操作符
     */
    public final String aggOp;

    public LogicalSelectListNode(String aggOp, String fieldName) {
        this.aggOp = aggOp;
        this.fieldName = fieldName;
    }
}
