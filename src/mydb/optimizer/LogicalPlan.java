package mydb.optimizer;

import mydb.ParsingException;
import mydb.common.Database;
import mydb.common.Type;
import mydb.execution.*;
import mydb.storage.*;
import mydb.transaction.Transaction;
import mydb.transaction.TransactionId;

import java.util.*;

/**
 * 逻辑查询计划。查询优化会将一棵语法分析树转换为逻辑查询计划。
 * LogicalPlan代表一个由Parser生产的逻辑查询计划，并可以交给优化器进行优化。
 * 一个逻辑查询计划包括一系列scan、join、filter操作结点，一个select列表和分组字段（GROUP BY）。
 * 目前LogicalPlan只能有一个聚合操作和一个分组字段。
 * LogicalPlan可以转换为一个物理查询计划，其优化过程使用JoinOptimizer选择最佳的JOIN。
 */
public class LogicalPlan {

    private List<LogicalJoinNode> joins;
    private final List<LogicalScanNode> tables;
    private final List<LogicalFilterNode> filters;

    /**
     * Key：表的别名；Value：对该表的操作符（如SeqScan对该表进行全表扫描）
     */
    private final Map<String, OpIterator> subplanMap;

    /**
     * 表名到表ID的映射（Key：tableAlias；Value：tableId）
     */
    private final Map<String, Integer> tableMap;

    /**
     * 需要进行投影或聚合操作的字段列表
     */
    private final List<LogicalSelectListNode> selectList;

    private String groupByField = null; // 分组字段
    private boolean hasAggregate = false; // 是否进行聚合操作
    private String aggregateOp;
    private String aggregateField; // 聚合字段

    private boolean hasOrderBy = false;
    private boolean orderByAsc; // 升序排序
    private String orderByField; // 排序字段

    private String query;

    /**
     * LogicalPlan构造函数，创建一个空的逻辑查询计划
     */
    public LogicalPlan() {
        this.joins = new ArrayList<>();
        this.filters = new ArrayList<>();
        this.tables = new ArrayList<>();
        this.subplanMap = new HashMap<>();
        this.tableMap = new HashMap<>();
        this.selectList = new ArrayList<>();
        this.query = "";
    }

    /**
     * 设置该逻辑查询计划的文本表示，该方法只是用于打印该查询计划所代表的文本
     * @param query 与该查询计划相关的文本
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * @return 返回该查询计划的文本表示
     */
    public String getQuery() {
        return this.query;
    }

    /**
     * 给定一个表的别名，返回该表对应的ID
     * @param tableAlias 表的别名
     * @return 该表对应的ID，如果没有该表则返回null
     */
    public Integer getTableId(String tableAlias) {
        return tableMap.get(tableAlias);
    }

    /**
     * @return 获取表的别名于表ID的映射Map
     */
    public Map<String, Integer> getTableAliasToIdMapping() {
        return this.tableMap;
    }

    /**
     * 将一个新的filter添加到该查询计划中
     * @param fieldName 进行过滤的字段名（字段名可以带有表的别名，也可以没有）
     * @param op 谓词操作符
     * @param constant 进行谓词比较的常数值
     * @throws ParsingException 字段不在表中或者该字段有歧义（两个表拥有同一个字段名）
     */
    public void addFilter(String fieldName, Predicate.Op op, String constant)
            throws ParsingException {
        fieldName = getFieldCompleteName(fieldName); // 获取带有表名的完整字段名
        String tableName = fieldName.split("[.]")[0];
        fieldName = fieldName.split("[.]")[1];
        LogicalFilterNode filterNode = new LogicalFilterNode(tableName, fieldName, op, constant);
        this.filters.add(filterNode);
    }

    /**
     * 添加一个对两个表的两个字段进行join的结点到该查询计划中
     * @param joinField1 进行连接的左表字段名
     * @param joinField2 进行连接的右表字段名
     * @param op 谓词操作符
     */
    public void addJoin(String joinField1, String joinField2, Predicate.Op op)
            throws ParsingException {
        joinField1 = getFieldCompleteName(joinField1);
        joinField2 = getFieldCompleteName(joinField2);
        String table1alias = joinField1.split("[.]")[0];
        String table2alias = joinField2.split("[.]")[0];
        // 不带表名的字段名
        String fieldName1 = joinField1.split("[.]")[1];
        String fieldName2 = joinField2.split("[.]")[1];
        if (table1alias.equals(table2alias)) {
            // 同一个表的连接操作会抛出异常
            throw new ParsingException("Cannot join on two fields from the same table.");
        }
        LogicalJoinNode joinNode = new LogicalJoinNode(table1alias, table2alias, fieldName1, fieldName2, op);
        System.out.println("Added join between " + joinField1 + " and " + joinField2);
        joins.add(joinNode);
    }

    /**
     * 对一个字段和一个子查询（subplan）结果进行JOIN
     * @param joinField1 字段名
     * @param joinField2 子查询结果字段
     * @param op 谓词操作符
     * @throws ParsingException 字段歧义会抛出异常
     */
    public void addJoin(String joinField1, OpIterator joinField2, Predicate.Op op)
            throws ParsingException {
        joinField1 = getFieldCompleteName(joinField1);
        String table1 = joinField1.split("[.]")[0]; // 表名（或别名）
        String field1 = joinField1.split("[.]")[1]; // 不带有表名的字段名
        LogicalSubplanJoinNode subplanJoinNode = new LogicalSubplanJoinNode(table1, field1, joinField2, op);
        joins.add(subplanJoinNode);
        System.out.println("Added subplan join on " + joinField1);
    }

    /**
     * 添加一个scan结点到该查询计划中
     * @param tableId 进行扫描的表的ID
     * @param tableAlias 表在该查询计划中的别名（alias）
     */
    public void addScan(int tableId, String tableAlias) {
        tables.add(new LogicalScanNode(tableId, tableAlias));
        tableMap.put(tableAlias, tableId);
        System.out.println("Added scan of table " + tableAlias);
    }

    /**
     * 添加需要进行投影（Project）或聚合（Aggregate）的字段到selectList中
     * @param fieldName 该查询计划中需要投影输出的字段
     * @param aggregateOp 聚合操作符
     */
    public void addProjectField(String fieldName, String aggregateOp)
            throws ParsingException {
        fieldName = getFieldCompleteName(fieldName); // 消除字段歧义
        if (fieldName.equals("*")) {
            fieldName = "null.*";
        }
        System.out.println("Added select list field " + fieldName);
        if (aggregateOp != null) {
            // 会对该字段进行聚合操作
            System.out.println("\t with aggregator " + aggregateOp);
        }
        this.selectList.add(new LogicalSelectListNode(aggregateOp, fieldName));
    }

    /**
     * 添加聚合操作到该查询计划中
     * @param aggregateOp 聚合操作符（MIN、MAX、COUNT等）
     * @param aggregateField 进行聚合的字段
     * @param groupByField 进行分组的字段
     */
    public void addAggregate(String aggregateOp, String aggregateField, String groupByField)
            throws ParsingException {
        // 获得带有表名的完整字段名
        aggregateField = getFieldCompleteName(aggregateField);
        if (groupByField != null) {
            groupByField = getFieldCompleteName(groupByField);
        }
        this.aggregateOp = aggregateOp;
        this.hasAggregate = true;
        this.aggregateField = aggregateField;
        this.groupByField = groupByField;
    }

    /**
     * 添加一个排序（ORDER BY）表达式到该查询计划中，对特定字段进行排序（只支持单个字段排序）
     * @param orderByField 进行排序的字段
     * @param asc true为升序排序，false为降序排序
     */
    public void addOrderBy(String orderByField, boolean asc)
            throws ParsingException {
        this.orderByField = getFieldCompleteName(orderByField);
        this.orderByAsc = asc;
        this.hasOrderBy = true;
    }

    /**
     * 给定一个字段名，尝试通过遍历所有表来得到该字段所属的表的表名，拼接形成完整字段名
     * @param fieldName 字段名（可选是否带有表名）
     * @return 返回完整字段名（tableAlias.fieldName）
     * @throws ParsingException 字段不在任何一个表中，或者字段有歧义（位于两个不同的表中）
     */
    String getFieldCompleteName(String fieldName) throws ParsingException {
        String[] fields = fieldName.split("[.]");
        if (fields.length > 2) {
            throw new ParsingException("Field " + fieldName + " is not a valid field reference.");
        }
        if (fields.length == 2) {
            if (!fields[0].equals("null")) {
                return fieldName;
            }
            // fields[0] == null
            fieldName = fields[1];
        }
        if (fieldName.equals("*")) {
            // SELECT *
            return fieldName; // "*"
        }
        Iterator<LogicalScanNode> tableIterator = tables.iterator();
        String tableName = null;
        while (tableIterator.hasNext()) {
            LogicalScanNode table = tableIterator.next();
            try {
                TupleDesc tupleDesc = Database.getCatalog().getDbFile(table.tableId).getTupleDesc();
                int tableId = tupleDesc.fieldNameToIndex(fieldName);
                if (tableName == null) {
                    tableName = table.tableAlias;
                } else {
                    // 字段名有歧义
                    throw new ParsingException("Field " + fieldName + " appears in multiple tables");
                }
            } catch (NoSuchElementException e) {
                // ignore
            }
        }
        if (tableName != null) {
            return tableName + "." +fieldName;
        }
        // 没有找到该字段名存在的表
        throw new ParsingException("Field " + fieldName + "does not appear in any tables.");
    }

    /**
     * 将字符串解析成对应的聚合操作符
     * @param str 待解析成聚合操作符的字符串
     * @throws ParsingException 未知的聚合操作符会抛出异常
     */
    static Aggregator.Op getAggregatorOp(String str) throws ParsingException {
        str = str.toUpperCase();
        switch (str) {
            case "AVG" -> {
                return Aggregator.Op.AVG;
            }
            case "MAX" -> {
                return Aggregator.Op.MAX;
            }
            case "MIN" -> {
                return Aggregator.Op.MIN;
            }
            case "SUM" -> {
                return Aggregator.Op.SUM;
            }
            case "COUNT" -> {
                return Aggregator.Op.COUNT;
            }
        }
        throw new ParsingException("Unknown aggregate operator " + str);
    }

    /**
     * 将一个逻辑计查询划转化为物理查询计划，其为最优的查询方案
     * @param tid 执行该查询计划的事务ID
     * @param tableStats 各个表的统计信息（基于直方图），用于计算查询成本选择最优的查询方案
     * @param explain 是否对该物理查询计划进行可视化解释
     * @return 输出该查询计划得到的各个元组（可利用Iterator遍历得到）
     * @throws ParsingException 若该逻辑查询计划无效回抛出解析异常
     */
    public OpIterator physicalPlan(TransactionId tid,
                                   Map<String, TableStats> tableStats,
                                   boolean explain)
            throws ParsingException {
        Iterator<LogicalScanNode> tableIterator = this.tables.iterator();
        Map<String, String> equivMap = new HashMap<>(); //
        Map<String, Double> filterSelectivitiesMap = new HashMap<>(); // 过滤选择性
        Map<String, TableStats> statsMap = new HashMap<>();
        // 遍历查询计划的每个表，把需要查询的每个表添加到subplan
        while (tableIterator.hasNext()) {
            LogicalScanNode table = tableIterator.next();
            SeqScan seqScan = null; // 创建一个全表扫描的操作符
            try {
                seqScan = new SeqScan(tid, Database.getCatalog().getDbFile(table.tableId).getId(), table.tableAlias);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown table " + table.tableId);
            }
            // 添加一个子查询
            this.subplanMap.put(table.tableAlias, seqScan);
            String tableName = Database.getCatalog().getTableName(table.tableId);
            statsMap.put(tableName, tableStats.get(tableName));
            // 暂时对每个表的过滤选择性设置为1.0
            filterSelectivitiesMap.put(table.tableAlias, 1.0);
        }
        // 获得过滤操作（WHERE子句）的选择性统计信息用于后续查询优化
        for (LogicalFilterNode filterNode: filters) {
            // WHERE子句的查询结果（元组集合）
            OpIterator subplan = subplanMap.get(filterNode.tableAlias);
            if (subplan == null) {
                throw new ParsingException("Unknown table in WHERE clause " + filterNode.tableAlias);
            }
            // 进行过滤操作（filter）的字段
            Field field = null;
            Type fieldType = null;
            TupleDesc tupleDesc = subplanMap.get(filterNode.tableAlias).getTupleDesc();
            try {
                // 获得字段索引
                int index = tupleDesc.fieldNameToIndex(filterNode.fieldCompleteName);
                fieldType = tupleDesc.getFieldType(index);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field in filter expression " + filterNode.fieldCompleteName);
            }
            if (fieldType.equals(Type.INT_TYPE)) {
                field = new IntField(Integer.parseInt(filterNode.constant));
            } else {
                // string type
                field = new StringField(filterNode.constant, Type.STRING_LEN);
            }
            Predicate predicate = null;
            try {
                int index = subplan.getTupleDesc().fieldNameToIndex(filterNode.fieldCompleteName);
                predicate = new Predicate(index, filterNode.op, field);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field " + filterNode.fieldCompleteName);
            }
            subplanMap.put(filterNode.tableAlias, new Filter(predicate, subplan));
            TableStats stats = statsMap.get(Database.getCatalog().getTableName(this.getTableId(filterNode.tableAlias)));
            int index = subplan.getTupleDesc().fieldNameToIndex(filterNode.fieldCompleteName);
            double selectivity = stats.estimateSelectivity(index, filterNode.op, field); // 选择性估计
            filterSelectivitiesMap.put(filterNode.tableAlias, selectivity);
        }

        // 连接优化器
        JoinOptimizer joinOptimizer = new JoinOptimizer(this, joins);

        for (LogicalJoinNode join: joins) {
            OpIterator plan1, plan2;
            boolean isSubplanJoin = join instanceof LogicalSubplanJoinNode;
            String leftTableName, rightTableName;
            // 判断是否为已连接的表
            if (equivMap.get(join.leftTableAlias) != null) {
                leftTableName = equivMap.get(join.leftTableAlias);
            } else {
                leftTableName = join.leftTableAlias;
            }
            if (equivMap.get(join.rightTableAlias) != null) {
                rightTableName = equivMap.get(join.rightTableAlias);
            } else {
                rightTableName = join.rightTableAlias;
            }
            plan1 = subplanMap.get(leftTableName);
            if (isSubplanJoin) {
                plan2 = ((LogicalSubplanJoinNode) join).subplan;
                if (plan2 == null) {
                    throw new ParsingException("Invalid subplan.");
                }
            } else {
                plan2 = subplanMap.get(rightTableName);
            }
            if (plan1 == null) {
                throw new ParsingException("Unknown table in WHERE clause " +
                        join.leftTableAlias);
            }
            if (plan2 == null) {
                throw new ParsingException("Unknown table in WHERE clause " +
                        join.rightTableAlias);
            }
            // 由JoinOptimizer获得最佳的连接顺序
            OpIterator j = JoinOptimizer.bestJoin(join, plan1, plan2);
            subplanMap.put(rightTableName, j);

            if (!isSubplanJoin) {
                subplanMap.remove(rightTableName);
                equivMap.put(rightTableName, leftTableName);
                for (Map.Entry<String, String> entry: equivMap.entrySet()) {
                    String value = entry.getValue();
                    if (value.equals(rightTableName)) {
                        entry.setValue(leftTableName);
                    }
                }
            }
        }
        if (subplanMap.size() > 1) {
            // subplanMap应该只剩下一个最终结果
            throw new ParsingException(
                    "Query does not include join expressions joining all nodes!");
        }
        // 唯一的操作结点
        OpIterator node = subplanMap.entrySet().iterator().next().getValue();
        // 进行输出的字段
        List<Integer> outFields = new ArrayList<>(); // 字段索引
        List<Type> outTypes = new ArrayList<>(); // 字段类型
        for (int i=0; i<selectList.size(); i++) {
            LogicalSelectListNode select = selectList.get(i);
            if (select.aggOp != null) {
                // 进行了聚合操作
                outFields.add(aggregateField != null ? 0 : 1);
                TupleDesc td = node.getTupleDesc();
                try {
                    // 判断是否有该字段
                    td.fieldNameToIndex(select.fieldName);
                } catch (NoSuchElementException e) {
                    throw new ParsingException("Unknown field " +  select.fieldName + " in SELECT list");
                }
                outTypes.add(Type.INT_TYPE);
            } else if (hasAggregate) {
                // 该查询计划进行了聚合操作
                if (groupByField == null) {
                    // 聚合操作需要有GROUP BY
                    throw new ParsingException(
                            "Field " + select.fieldName + " does not appear in GROUP BY list");
                }
                outFields.add(0);
                TupleDesc td = node.getTupleDesc();
                int fieldIndex;
                try {
                    fieldIndex = td.fieldNameToIndex(groupByField);
                } catch (NoSuchElementException e) {
                    throw new ParsingException(
                            "Unknown field " + groupByField + " in GROUP BY statement");
                }
                outTypes.add(td.getFieldType(fieldIndex));
            } else if (select.fieldName.equals("null.*")) {
                // SELECT *
                TupleDesc td = node.getTupleDesc();
                for (i=0; i<td.getFieldsNum(); i++) {
                    outFields.add(i);
                    outTypes.add(td.getFieldType(i));
                }
            } else {
                TupleDesc td = node.getTupleDesc();
                int fieldIndex;
                try {
                    fieldIndex = td.fieldNameToIndex(select.fieldName);
                } catch (NoSuchElementException e) {
                    throw new ParsingException("Unknown field " +  select.fieldName + " in SELECT list");
                }
                outFields.add(fieldIndex);
                outTypes.add(td.getFieldType(fieldIndex));
            }
        }

        if (hasAggregate) {
            // 聚合操作
            TupleDesc td = node.getTupleDesc();
            Aggregate aggregateNode;
            try {
                aggregateNode = new Aggregate(node, td.fieldNameToIndex(aggregateField),
                        groupByField == null ? -1: td.fieldNameToIndex(groupByField),
                        getAggregatorOp(aggregateOp));
            } catch (NoSuchElementException e) {
                throw new mydb.ParsingException(e);
            }
            node = aggregateNode;
        }

        if (hasOrderBy) {
            // 排序操作
            node = new OrderBy(node.getTupleDesc().fieldNameToIndex(orderByField), this.orderByAsc, node);
        }

        // 将逻辑查询计划转换为物理查询计划，返回投影（Project）结点为最终SELECT输出的元组
        return new Project(outFields, outTypes, node);
    }
}
