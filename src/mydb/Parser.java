package mydb;

// 一个Java实现的SQL解析jar包
import Zql.*;
// 用于实现Java命令行交互
import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Type;
import mydb.execution.*;
import mydb.optimizer.LogicalPlan;
import mydb.optimizer.TableStats;
import mydb.storage.IntField;
import mydb.storage.StringField;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.Transaction;
import mydb.transaction.TransactionException;
import mydb.transaction.TransactionId;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * SQL解析器
 */
public class Parser {

    /**
     * 解释模式
     */
    static boolean explain = false;

    /**
     * 当前正在执行的事务
     */
    private Transaction currentTransaction;

    /**
     * 判断是否有事务正在执行的标志
     */
    private boolean isTransactionExecuting;

    public static Predicate.Op getOp(String str) throws ParsingException {
        if (str.equals("="))
            return Predicate.Op.EQUALS;
        if (str.equals(">"))
            return Predicate.Op.GREATER_THAN;
        if (str.equals(">="))
            return Predicate.Op.GREATER_THAN_OR_EQ;
        if (str.equals("<"))
            return Predicate.Op.LESS_THAN;
        if (str.equals("<="))
            return Predicate.Op.LESS_THAN_OR_EQ;
        if (str.equals("LIKE"))
            return Predicate.Op.LIKE;
        if (str.equals("~"))
            return Predicate.Op.LIKE;
        if (str.equals("!="))
            return Predicate.Op.NOT_EQUALS;
        if (str.equals("<>"))
            return Predicate.Op.NOT_EQUALS;
        throw new ParsingException("unknown predicate " + str);
    }

    /**
     * 处理解析得到的表达式（可能包含AND、OR等表达式运算符）。
     * Example: a AND b AND c -> operator = AND, operands = (a, b, c)
     */
    void processExpression(TransactionId tid, ZExpression expression, LogicalPlan logicalPlan)
            throws ParsingException, IOException, Zql.ParseException {
        if (expression.getOperator().equals("AND")) {
            // 遍历所有操作数（number of operands）
            for (int i=0; i<expression.nbOperands(); i++) {
                if (!(expression.getOperand(i) instanceof ZExpression)) {
                    throw new ParsingException("Nested queries are currently unsupported");
                }
                ZExpression newExpression = (ZExpression) expression.getOperand(i);
                processExpression(tid, newExpression, logicalPlan);
            }
        } else if (expression.getOperator().equals("OR")) {
            throw new ParsingException("OR expressions currently unsupported.");
        } else {
            // 二元表达式（比较两个操作数）
            List<ZExp> operands = expression.getOperands();
            if (operands.size() != 2) {
                // 目前仅支持二元表达式
                String msg = "Only simple binary expressions of the form A op B are currently supported";
                throw new ParsingException(msg);
            }
            ZExp a = operands.get(0);
            ZExp b = operands.get(1);

            boolean isJoin = false; // 用于判断表达式是否为field1=field2的形式，即是否为连接操作
            Predicate.Op op = getOp(expression.getOperator()); // 比较操作符
            if (a instanceof ZConstant && b instanceof ZConstant) {
                // 两个都是字段名则为进行JOIN操作
                isJoin = ((ZConstant) a).getType() == ZConstant.COLUMNNAME && ((ZConstant) b).getType() == ZConstant.COLUMNNAME;
            } else if (a instanceof ZQuery || b instanceof ZQuery) {
                // 至少一个操作数为查询操作，必定为JOIN
                isJoin = true;
            } else if (a instanceof ZExpression || b instanceof ZExpression) {
                // 不支持嵌套表达式，即三元及以上的表达式
                String msg = "Only simple binary expressions of the form A op B are currently supported, where A or B are fields, constants, or sub queries.";
                throw new ParsingException(msg);
            } else {
                isJoin = false;
            }

            if (isJoin) {
                // join node
                String leftTableField = "";
                String rightTableField = "";
                if (!(a instanceof ZConstant)) {
                    // 左操作数是嵌套查询，无效
                } else {
                    leftTableField = ((ZConstant) a).getValue();
                }
                if (!(b instanceof ZConstant)) {
                    // 右操作数是嵌套查询
                    LogicalPlan subPlan = parseQueryLogicalPlan(tid, (ZQuery) b);
                    OpIterator physicalPlan = subPlan.physicalPlan(tid, TableStats.getStatsMap(), explain);
                    logicalPlan.addJoin(leftTableField, physicalPlan, op);
                } else {
                    rightTableField = ((ZConstant) b).getValue();
                    logicalPlan.addJoin(leftTableField, rightTableField, op);
                }
            } else {
                // isJoin == false, select node
                String field;
                String compareValue;
                ZConstant operand1 = (ZConstant) a;
                ZConstant operand2 = (ZConstant) b;
                // 判断比较符的哪一边是字段，那一边是比较的值
                if (operand1.getType() == ZConstant.COLUMNNAME) {
                    field = operand1.getValue();
                    compareValue = operand2.getValue();
                } else {
                    field = operand2.getValue();
                    compareValue = operand1.getValue();
                }
                logicalPlan.addFilter(field, op, compareValue);
            }
        }
    }

    public static final String[] SQL_COMMANDS = {
            "select", "from", "where", "insert", "delete", "commit", "rollback",
            "group by", "max(", "min(", "avg(", "count", "values", "into"
    };

    public static void main(String[] argv)
            throws IOException {
        if (argv.length < 1 || argv.length > 4) {
            System.out.println("Invalid number of arguments\n");
            System.exit(0);
        }
        // SQL解析器，启动！
        Parser parser = new Parser();
        parser.start(argv);
    }

    // 命令行使用提示
    static final String usage = "Usage: parser catalogFile [-explain] [-f queryFile]";

    static final int SLEEP_TIME = 1000; // 1000ms

    /**
     * 交互模式（是否使用命令行）
     */
    protected boolean interactive = true;

    /**
     * 开启SQL解析器
     */
    protected void start(String[] argv) throws IOException {
        // 首先将表添加到数据库中
        Database.getCatalog().loadSchema(argv[0]);
        TableStats.computeStats(); // 计算统计信息，用于查询优化

        String queryFile = null;

        if (argv.length > 1) {
            for (int i=1; i<argv.length; i++) {
                if (argv[i].equals("-explain")) {
                    // 开启解释模式
                    explain = true;
                    System.out.println("Explain mode enabled.");
                } else if (argv[i].equals("-f")) {
                    interactive = false;
                    // 判断后面是否为命令结尾
                    if (i++ == argv.length) {
                        // -f后面没有具体文件名
                        System.out.println("Expected file name after -f\n" + usage);
                        System.exit(0);
                    }
                    queryFile = argv[i];
                } else {
                    System.out.println("Unknown argument " + argv[i] + "\n" + usage);
                }
            }
        }
        if (!interactive) {
            // 未进入命令行交互模式，直接打开SQL文件进行查询
            try {
                try {
                    Thread.sleep(SLEEP_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long startTime = System.currentTimeMillis();
                processNextStatement(new FileInputStream(queryFile));
                long time = System.currentTimeMillis() - startTime; // 处理一个查询文件的总耗时
                System.out.printf("----------------\n%.2f seconds\n\n", ((double) time / 1000.0));
                System.out.println("Press Enter to exit");
                this.shutdown(); // 关闭解析器
            } catch (FileNotFoundException e) {
                System.out.println("Unable to find the query file " + queryFile);
                e.printStackTrace();
            }
        } else {
            // interactive=true，进入命令行交互模式
            // 读取控制台命令行输入的内容
            // ConsoleReader reader = new ConsoleReader();
            Scanner scanner = new Scanner(System.in);
            // 可以利用tab进行SQL命令的自动补全功能
            ArgumentCompletor completor = new ArgumentCompletor(
                    new SimpleCompletor(SQL_COMMANDS));
            // 关闭严格模式，可自动补全包含输入内容的任何可能项（严格模式只匹配开头）
            completor.setStrict(false);

            StringBuilder buffer = new StringBuilder();
            String line = null;
            boolean quit = false;
            while (!quit) {
                System.out.print("MyDB> ");
                if (scanner.hasNextLine()) {
                    line = scanner.nextLine();
                }
                //line = reader.readLine("MyDB> ");
                if (line == null) {
                    break;
                }
                // 利用分号分隔每一条SQL命令
                while (line.indexOf(';') >= 0) {
                    // 直到遇到分号才识别出一条完整的SQL命令
                    int splitPos = line.indexOf(';');
                    buffer.append(line, 0, splitPos + 1);
                    String cmd = buffer.toString().trim();
                    cmd = cmd.substring(0, cmd.length() - 1).trim() + ";";
                    // 利用UTF-8编码格式将SQL命令转换成二进制形式
                    byte[] statementBytes = cmd.getBytes(StandardCharsets.UTF_8);
                    if (cmd.equalsIgnoreCase("quit;") ||
                            cmd.equalsIgnoreCase("exit;")) {
                        shutdown(); // 关闭数据库
                        quit = true;
                        break;
                    }
                    long startTime = System.currentTimeMillis();
                    processNextStatement(new ByteArrayInputStream(statementBytes));
                    // 完成SQL命令消耗的总时间
                    long time = System.currentTimeMillis() - startTime;
                    System.out.printf("----------------\n%.2f seconds\n\n",
                            ((double) time / 1000.0));
                    // 截取剩余的未执行的SQL命令
                    line = line.substring(splitPos + 1);
                    buffer = new StringBuilder();
                }
                if (line.length() > 0) {
                    buffer.append(line);
                    buffer.append("\n");
                }
            }
            scanner.close();
        }

    }

    /**
     * 关闭SQL解析器
     */
    protected void shutdown() throws IOException {
        System.out.println("bye");
    }

    public void processNextStatement(String s) {
        processNextStatement(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
    }

    public void processNextStatement(InputStream inputStream) {
        try {
            ZqlParser zqlParser = new ZqlParser(inputStream);
            ZStatement statement = zqlParser.readStatement();
            Query query = null;
            if (statement instanceof ZTransactStmt) {
                // 处理事务语句（COMMIT, ROLLBACK, SET TRANSACTION）
                processTransactionStatement((ZTransactStmt) statement);
            } else {
                if (!this.isTransactionExecuting) {
                    // 无事务正在执行，创建一个新的事务
                    currentTransaction = new Transaction();
                    currentTransaction.start();
                    System.out.println("Started a new transaction id = " +
                            currentTransaction.getId().getId());
                }
                try {
                    // 判断解析得到的statement语句类型
                    if (statement instanceof ZInsert) {
                        query = processInsertStatement((ZInsert) statement, currentTransaction.getId());
                    } else if (statement instanceof ZDelete) {
                        query = processDeleteStatement((ZDelete) statement, currentTransaction.getId());
                    } else if (statement instanceof ZQuery) {
                        query = processQueryStatement((ZQuery) statement, currentTransaction.getId());
                    } else {
                        // 提示只能解析INSERT、DELETE、SELECT语句
                        String msg = "parser only handles SQL transactions, insert, delete, and select statements";
                        System.out.println("Can't parse " + statement + "\n -- " + msg);
                    }
                    if (query != null) {
                        // 进行了SELECT操作，有查询请求
                        query.execute();
                    }
                    if (!isTransactionExecuting && currentTransaction != null) {
                        // 事务执行正常结束，需要COMMIT
                        currentTransaction.commit();
                        long tid = currentTransaction.getId().getId();
                        System.out.println("Transaction id " + tid + " committed.");
                    }
                } catch (Throwable a) {
                    // 无论是否发生error，事务都处理为abort（未正常结束），需要ROLLBACK
                    if (currentTransaction != null) {
                        currentTransaction.rollback(); // abort and rollback
                        long tid = currentTransaction.getId().getId();
                        System.out.println("Transaction id " + tid + " aborted.");
                    }
                    this.isTransactionExecuting = false; // 设置为没有事务正在执行
                    if (a instanceof ParsingException || a instanceof Zql.ParseException) {
                        // SQL解析错误
                        throw new ParsingException((Exception) a);
                    }
                    if (a instanceof Zql.TokenMgrError) {
                        // 定位发生token错误的位置
                        throw new DbException(a.getMessage());
                    }
                } finally {
                    if (!isTransactionExecuting) {
                        this.currentTransaction = null;
                    }
                }
            }
        } catch (IOException | DbException e) {
            e.printStackTrace();
        } catch (ParsingException e) {
            System.out.println("Invalid SQL expression: \n \t" + e.getMessage());
        } catch (ParseException | TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
        }
    }

    public void processTransactionStatement(ZTransactStmt stmt)
            throws IOException, ParsingException {
        switch (stmt.getStmtType()) {
            case "COMMIT" -> {
                if (currentTransaction == null) {
                    // 没有正在执行的事务
                    throw new ParsingException("No transaction is currently executing");
                }
                // 提交事务
                currentTransaction.commit();
                currentTransaction = null;
                isTransactionExecuting = false;
                System.out.println("Transaction " + currentTransaction.getId().getId() + " committed");
            }
            case "ROLLBACK" -> {
                if (currentTransaction == null) {
                    // 没有正在执行的事务
                    throw new ParsingException("No transaction is currently executing");
                }
                // 事务执行失败，进行回滚
                currentTransaction.rollback();
                currentTransaction = null;
                isTransactionExecuting = false;
                System.out.println("Transaction " + currentTransaction.getId().getId() + " aborted");
            }
            case "SET TRANSACTION" -> {
                if (currentTransaction != null) {
                    // 有事务正在执行，无法SET TRANSACTION
                    throw new ParsingException("Can't start a new transactions until current transaction has been committed or aborted");
                }
                // 创建新的事务
                currentTransaction = new Transaction();
                currentTransaction.start();
                isTransactionExecuting = true;
                System.out.println("Started a new transaction id = " + currentTransaction.getId().getId());
            }
            default -> {
                throw new ParsingException("Unsupported operation");
            }
        }
    }

    /**
     * 处理解析得到的Insert语句
     */
    public Query processInsertStatement(ZInsert statement, TransactionId tid)
            throws DbException, IOException, ParsingException, Zql.ParseException {
        int tableId;
        try {
            tableId = Database.getCatalog().getTableId(statement.getTable());
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown table: " + statement.getTable());
        }

        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        Tuple tuple = new Tuple(tupleDesc); // 待添加字段的元组
        int i = 0; // 字段索引
        OpIterator newTuples = null;
        if (statement.getValues() != null) {
            // 获得INSERT语句的相关元组的值
            List<ZExp> values = statement.getValues();
            if (tupleDesc.getFieldsNum() != values.size()) {
                String msg = "INSERT statement does not contain the same number of fields as table: " + statement.getTable();
                throw new ParsingException(msg);
            }
            // 遍历一个元组所有字段
            for (ZExp exp: values) {
                if (!(exp instanceof ZConstant)) {
                    throw new ParsingException("Complex expressions not allowed in INSERT statements");
                }
                ZConstant constant = (ZConstant) exp;
                if (constant.getType() == ZConstant.NUMBER) {
                    if (tupleDesc.getFieldType(i) != Type.INT_TYPE) {
                        // 字段类型应该与解析得到的类型一致
                        String msg = "Value " + constant.getValue() + " is not an integer, expected a string";
                        throw new ParsingException(msg);
                    }
                    IntField intField = new IntField(Integer.parseInt(constant.getValue()));
                    tuple.setField(i, intField);
                } else if (constant.getType() == ZConstant.STRING) {
                    if (tupleDesc.getFieldType(i) != Type.STRING_TYPE) {
                        String msg = "Value " + constant.getValue() + " is not a string, expected an integer";
                        throw new ParsingException(msg);
                    }
                    StringField stringField = new StringField(constant.getValue(), Type.STRING_LEN);
                    tuple.setField(i, stringField);
                } else {
                    String msg = "Only string or int fields are supported.";
                    throw new ParsingException(msg);
                }
                i++; // field index
            }
            List<Tuple> tuples = new ArrayList<>();
            tuples.add(tuple);
            newTuples = new TupleArrayIterator(tuples);

        } else {
            // statement.getValues() == null
            ZQuery zQuery = statement.getQuery();
            LogicalPlan logicalPlan = parseQueryLogicalPlan(tid, zQuery);
            // 转化为物理计划，新增INSERT的元组
            newTuples = logicalPlan.physicalPlan(tid, TableStats.getStatsMap(), explain);
        }
        Query query = new Query(tid);
        query.setPhysicalPlan(new Insert(tid, newTuples, tableId));
        return query;
    }

    /**
     * 处理解析得到的Delete语句
     */
    public Query processDeleteStatement(ZDelete statement, TransactionId tid)
            throws DbException, IOException, ParsingException, Zql.ParseException {
        int tableId;
        try {
            tableId = Database.getCatalog().getTableId(statement.getTable());
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown table : " + statement.getTable());
        }
        String tableName = statement.getTable();
        Query query = new Query(tid);
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.setQuery(statement.toString());
        logicalPlan.addScan(tableId, tableName);
        if (statement.getWhere() != null) {
            // 处理WHERE子句的表达式
            processExpression(tid, (ZExpression) statement.getWhere(), logicalPlan);
        }
        // 投影
        logicalPlan.addProjectField("null.*", null);
        OpIterator opIterator = new Delete(tid, logicalPlan.physicalPlan(tid, TableStats.getStatsMap(), false));
        query.setPhysicalPlan(opIterator);
        return query;
    }

    /**
     * 处理解析得到的Select语句（查询请求）
     */
    public Query processQueryStatement(ZQuery statement, TransactionId tid)
        throws IOException, ParsingException, Zql.ParseException {
        Query query = new Query(tid);
        LogicalPlan logicalPlan = parseQueryLogicalPlan(tid, statement);
        OpIterator physicalPlan = logicalPlan.physicalPlan(tid, TableStats.getStatsMap(), explain); // 逻辑查询计划转换为物理计划
        query.setLogicalPlan(logicalPlan);
        query.setPhysicalPlan(physicalPlan);
        if (physicalPlan != null) {
            Class<?> c;
            try {
                // 更新操作符基数
                c = Class.forName("mydb.optimizer.OperatorCardinality");
                Class<?> op = Operator.class;
                Class<?> map = Map.class;
                // 反射调用成员方法
                java.lang.reflect.Method method = c.getMethod("updateOperatorCardinality", op, map, map);
                System.out.println("The query plan is:");
                method.invoke(null, physicalPlan, logicalPlan.getTableAliasToIdMapping(), TableStats.getStatsMap());
                // TODO SQL树可视化
            } catch (ClassNotFoundException | SecurityException ignored) {
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return query;
    }

    public LogicalPlan parseQueryLogicalPlan(TransactionId tid, ZQuery zQuery)
            throws IOException,ParsingException, Zql.ParseException {
        List<ZFromItem> fromTables = zQuery.getFrom(); // FROM的表
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.setQuery(zQuery.toString());
        // 遍历FROM子句中的表
        for (int i=0; i<fromTables.size(); i++) {
            ZFromItem item = fromTables.get(i);
            try {
                int tableId = Database.getCatalog().getTableId(item.getTable()); // 若表ID不存在会抛出异常
                String tableName;
                if (item.getAlias() != null) {
                    // 该表有别名
                    tableName = item.getAlias();
                } else {
                    tableName = item.getTable();
                }
                logicalPlan.addScan(tableId, tableName);
            } catch (NoSuchElementException e) {
                e.printStackTrace();
                String msg = "Table " + item.getTable() + " is not in the catalog";
                throw new ParsingException(msg);
            }
        }

        // 解析WHERE子句，创建Filter和Join操作符
        ZExp where = zQuery.getWhere();
        if (where != null) {
            if (!(where instanceof ZExpression)) {
                // 暂时不支持嵌套查询
                throw new ParsingException("Nested queries are currently unsupported");
            }
            // 处理WHERE子句的表达式
            ZExpression expression = (ZExpression) where;
            processExpression(tid, expression, logicalPlan);
        }

        // 处理GROUP BY
        ZGroupBy groupBy = zQuery.getGroupBy();
        String groupByField = null;
        if (groupBy != null) {
            // 存在GROUP BY
            List<ZExp> groupByList = groupBy.getGroupBy();
            if (groupByList.size() > 1) {
                // 暂时不支持GROUP BY多个字段
                String msg = "At most one grouping field expression supported";
                throw new ParsingException(msg);
            }
            if (groupByList.size() == 1) {
                // 获取GROUP BY的字段
                ZExp exp = groupByList.get(0);
                if (!(exp instanceof ZConstant)) {
                    String msg = "Complex grouping expressions (" + exp + ") not supported";
                    throw new ParsingException(msg);
                }
                groupByField = ((ZConstant) exp).getValue();
                System.out.println("GROUP BY FIELD: " + groupByField);
            }
        }

        // 遍历SELECT列表进行聚合操作
        List<ZSelectItem> selectList = zQuery.getSelect();
        String aggregateField = null; // 聚合字段
        String aggregateFunc = null; // 聚合函数

        for (int i=0; i<selectList.size(); i++) {
            ZSelectItem selectItem = selectList.get(i);
            if (selectItem.getAggregate() == null &&
                    selectItem.isExpression() &&
                    !(selectItem.getExpression() instanceof ZConstant)) {
                // SELECT语句暂不支持表达式
                throw new ParsingException("Expressions in SELECT list are not supported");
            }
            if (selectItem.getAggregate() != null) {
                // SELECT列表含有聚合操作
                if (aggregateField != null) {
                    throw new ParsingException("Aggregates over multiple fields not supported");
                }
                aggregateField = ((ZConstant)((ZExpression) selectItem.getExpression()).getOperand(0)).getValue();
                aggregateFunc = selectItem.getAggregate();
                System.out.println("Aggregate field: " + aggregateField +  ", aggregate function: " + aggregateFunc);
                logicalPlan.addProjectField(aggregateField, aggregateFunc);
            } else {
                // 字段带有表别名（Column指字段）
                if (groupByField != null &&
                        !(groupByField.equals(selectItem.getTable() + "."
                                + selectItem.getColumn()) || groupByField.equals(selectItem
                                .getColumn()))) {
                    throw new ParsingException("Non-aggregate field "
                            + selectItem.getColumn()
                            + " does not appear in GROUP BY list");
                }
                logicalPlan.addProjectField(selectItem.getTable() + "." + selectItem.getColumn(), null);
            }
        }

        if (groupByField != null && aggregateFunc == null) {
            // 只有分组字段但没有聚合函数
            throw new ParsingException("GROUP BY without aggregation");
        }

        if (aggregateFunc != null) {
            // 聚合函数
            logicalPlan.addAggregate(aggregateFunc, aggregateField, groupByField);
        }

        // 处理ORDER BY
        if (zQuery.getGroupBy() != null) {
            List<ZOrderBy> orderByList = zQuery.getOrderBy();
            if (orderByList.size() > 1) {
                // 暂不支持多字段排序
                throw new ParsingException("Multi-attribute ORDER BY is not supported");
            }
            ZOrderBy orderBy = orderByList.get(0);
            if (!(orderBy.getExpression() instanceof ZConstant)) {
                throw new ParsingException("Complex ORDER BY's are not supported");
            }
            ZConstant field = (ZConstant) orderBy.getExpression(); // 进行排序的字段
            logicalPlan.addOrderBy(field.getValue(), orderBy.getAscOrder());
        }
        return logicalPlan;
    }

    /**
     * 根据SQL语句生成对应的逻辑查询计划
     * @param tid 事务ID
     * @param statement SQL语句
     */
    public LogicalPlan generateLogicalPlan(TransactionId tid, String statement)
            throws ParsingException, IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(statement.getBytes());
        ZqlParser zqlParser = new ZqlParser(bis);
        try {
            // 读取下一条SQL语句
            ZStatement stmt = zqlParser.readStatement();
            if (stmt instanceof ZQuery) {
                // SELECT
                return parseQueryLogicalPlan(tid, (ZQuery) stmt);
            }
        } catch (Zql.ParseException e) {
            throw new ParsingException("Invalid SQL statement: \n \t " + e);
        }
        throw new ParsingException("Cannot generate logical plan for statement: " + statement);
    }


    public void setTransaction(Transaction t) {
        this.currentTransaction = t;
    }

    public Transaction getTransaction() {
        return this.currentTransaction;
    }


    class TupleArrayIterator implements OpIterator {

        @Serial
        private static final long serialVersionUID = 1L;

        final List<Tuple> tuples;

        Iterator<Tuple> tupleIterator = null;

        public TupleArrayIterator(List<Tuple> tuples) {
            this.tuples = tuples;
        }

        @Override
        public void open() throws DbException, TransactionException {
            tupleIterator = tuples.iterator();
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() throws DbException, TransactionException {
            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, NoSuchElementException, TransactionException {
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionException {
            tupleIterator = tuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tuples.get(0).getTupleDesc();
        }
    }
}
