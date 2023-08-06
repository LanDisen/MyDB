package mydb.optimizer;

import mydb.common.Catalog;
import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Type;
import mydb.execution.Predicate;
import mydb.execution.SeqScan;
import mydb.storage.*;
import mydb.transaction.Transaction;
import mydb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * TableStats用于表示一个查询计划中所操作的表的统计信息（表的元组总数、页面数量、直方图等）
 */
public class TableStats {

    /**
     * Key：表名；Value：对应表的统计信息
     */
    private static final ConcurrentHashMap<String, TableStats> statsMap =
            new ConcurrentHashMap<>();

    public static int IO_COST_PER_PAGE = 1000; // 每个页的IO成本

    public static TableStats getTableStats(String tableName) {
        return statsMap.get(tableName);
    }

    public static void setTableStats(String tableName, TableStats tableStats) {
        statsMap.put(tableName, tableStats);
    }

    public static void setTableStats(Map<String, TableStats> stats) {
        try {
            // java反射，获取TableStats的静态成员属性statsMap
            java.lang.reflect.Field statsMapField = TableStats.class.getDeclaredField("statsMap");
            statsMapField.setAccessible(true); // 即使private也可以访问statsMap成员属性
            statsMapField.set(null, stats); // null表示为静态成员属性
        } catch (NoSuchFieldException | IllegalAccessException |
                 IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStats() {
        Iterator<Integer> tableIdIterator = Database.getCatalog().tableIdIterator();
        System.out.println("Computing table stats.");
        while (tableIdIterator.hasNext()) {
            int tableId = tableIdIterator.next();
            TableStats tableStats = new TableStats(tableId, IO_COST_PER_PAGE);
            setTableStats(Database.getCatalog().getTableName(tableId), tableStats);
        }
        System.out.println("Done.");
    }

    /**
     * 直方图的桶数量，可任意调整
     */
    static final int HIST_BUCKETS_NUM = 100;

    private final int tableId;

    /**
     * 该表中每页的IO成本
     */
    private final int ioCostPerPage;

    // 记录表中每个字段的直方图
    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StrHistogram> strHistogramMap;

    /**
     * 表的页面数量
     */
    private int pageNum;

    private TupleDesc tupleDesc;

    private int tuplesTotalNum;


    /**
     * TableStats构造函数，创建一个实例用来跟踪表中每一列的统计信息
     * @param tableId 需要计算统计信息的表ID
     * @param ioCostPerPage 每一个页面的IO成本，不区分顺序扫描的IO和磁盘寻道IO
     */
    public TableStats(int tableId, int ioCostPerPage) {
        this.tableId = tableId;
        this.ioCostPerPage = ioCostPerPage;
        this.tuplesTotalNum = 0;
        this.intHistogramMap = new ConcurrentHashMap<>();
        this.strHistogramMap = new ConcurrentHashMap<>();
        // 根据tableId获得对应的数据库文件
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        this.pageNum = ((HeapFile) dbFile).getPagesNum();
        this.tupleDesc = dbFile.getTupleDesc();
        // 获得表的DbFile，然后进行全表扫描（不止一次扫描），计算所需要的统计信息
        try {
            initHistogram(tableId);
        }  catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表的信息初始化直方图
     * @param tableId 表ID
     */
    private void initHistogram(int tableId)
            throws DbException {
        // 第一次全表扫描首先获得表的最大最小值
        int fieldsNum = tupleDesc.getFieldsNum();
        // 创建一个事务进行全表扫描
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, tableId);
        // 记录每个字段的最小值和最大值
        Map<Integer, Integer> minMap = new HashMap<>();
        Map<Integer, Integer> maxMap = new HashMap<>();
        while (seqScan.hasNext()) {
            Tuple tuple = seqScan.next();
            this.tuplesTotalNum++;
            // 遍历每个字段
            for (int i=0; i<fieldsNum; i++) {
                Type type = this.tupleDesc.getFieldType(i);
                if (type.equals(Type.INT_TYPE)) {
                    // int类型
                    IntField field = (IntField) tuple.getField(i);
                    Integer minValue = minMap.getOrDefault(i, Integer.MAX_VALUE);
                    minMap.put(i, Math.min(minValue, field.getValue()));
                    Integer maxValue = maxMap.getOrDefault(i, Integer.MIN_VALUE);
                    maxMap.put(i, Math.max(maxValue, field.getValue()));
                } else {
                    // str类型
                    StrHistogram histogram = this.strHistogramMap.getOrDefault(i, new StrHistogram(HIST_BUCKETS_NUM));
                    StringField field = (StringField) tuple.getField(i);
                    histogram.addValue(field.getValue());
                    this.strHistogramMap.put(i, histogram);
                }
            }
        }
        // 获得每个字段的最值后，实例化每个字段的直方图
        for (int i=0; i<fieldsNum; i++) {
            if (minMap.get(i) != null) {
                Integer min = minMap.get(i);
                Integer max = maxMap.get(i);
                this.intHistogramMap.put(i,
                        new IntHistogram(HIST_BUCKETS_NUM, min, max));
            }
        }

        // 第二次全表扫描，计算直方图的统计信息
        seqScan.rewind();
        while (seqScan.hasNext()) {
            Tuple tuple = seqScan.next();
            // 遍历表的每个字段
            for (int i=0; i<fieldsNum; i++) {
                Type type = this.tupleDesc.getFieldType(i);
                if (type.equals(Type.INT_TYPE)) {
                    IntField field = (IntField) tuple.getField(i);
                    IntHistogram intHistogram = this.intHistogramMap.get(i);
                    if (intHistogram == null) {
                        throw new IllegalArgumentException("Illegal argument");
                    }
                    intHistogram.addValue(field.getValue());
                    this.intHistogramMap.put(i, intHistogram);
                }
            }
        }
        seqScan.close();
    }

    /**
     * 给定读取页面的IO成本，估计顺序扫描的成本。
     * 假设磁盘一次性读取整个页面，所以即使表的最后一个页面只有一个元组，读取该页面的成本和读取整个页面的成本一样高
     * @return 返回顺序扫描该表的估计成本
     */
    public double estimateScanCost() {
        // 进行了两次全表扫描（第一次得到最值，第二次统计信息）
        return pageNum * ioCostPerPage * 2;
    }

    /**
     * 给定某个谓词的选择度（selectivity），估计一个关系的元组数量（基数）
     * @param selectivity 某个谓词得到的选择度（selectivity）
     * @return 返回扫描的估计基数（元组总数乘以选择度）
     */
    public int estimateTableCardinality(double selectivity) {
        return (int) (tuplesTotalNum * selectivity);
    }

    /**
     * 给定字段和谓词估计得到平均选择度
     * @param fieldIndex 字段索引
     * @param op 谓词操作符（Predicate.Op）
     * @return 返回所估计的该表的平均选择度（使用直方图进行估计）
     */
    public double avgSelectivity(int fieldIndex, Predicate.Op op) {
        Type type = this.tupleDesc.getFieldType(fieldIndex);
        if (type.equals(Type.INT_TYPE)) {
            return intHistogramMap.get(fieldIndex).avgSelectivity();
        } else {
            return strHistogramMap.get(fieldIndex).avgSelectivity();
        }
    }

    /**
     * 给定谓词以估计该表某个字段的选择度（selectivity）
     * @param fieldIndex 谓词所操作的字段索引
     * @param op 谓词操作符（Predicate.Op）
     * @param constant 进行谓词比较的值
     * @return 返回所估计的选择度（selectivity）
     */
    public double estimateSelectivity(int fieldIndex, Predicate.Op op, Field constant) {
        Type type = this.tupleDesc.getFieldType(fieldIndex);
        if (type.equals(Type.INT_TYPE)) {
            return intHistogramMap.get(fieldIndex).estimateSelectivity(
                    op, ((IntField) constant).getValue());
        } else {
            return strHistogramMap.get(fieldIndex).estimateSelectivity(
                    op, ((StringField) constant).getValue());
        }
    }

    /**
     * @return 返回该表的元组总数量（总行数）
     */
    public int getTuplesTotalNum() {
        return tuplesTotalNum;
    }
}
