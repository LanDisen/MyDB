package mydb.optimizer;

/**
 * Scan结点，表示一个逻辑查询计划中FROM list中的表
 */
public class LogicalScanNode {

    /**
     * 表ID
     */
    public final int tableId;

    /**
     * 在查询计划中使用的表的别名
     */
    public final String tableAlias;

    public LogicalScanNode(int tableId, String tableAlias) {
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }
}
