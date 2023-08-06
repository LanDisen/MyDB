package mydb.optimizer;

import mydb.execution.Predicate;

/**
 * 基于int类型字段的固定宽度直方图
 */
public class IntHistogram {

    /**
     * 桶的集合，用于存储每个桶中放置的元组数量
     */
    private int[] buckets;

    /**
     * 直方图中桶的数量
     */
    private final int bucketsNum;

    /**
     * 该直方图的最小值
     */
    private final int min;

    /**
     * 该直方图的最大值
     */
    private final int max;

    /**
     * 该直方图每个桶的宽度
     */
    private final double width;

    /**
     * 元组总数量
     */
    private int tuplesTotalNum;

    /**
     * IntHistogram构造函数，用于维护一个直方图用于接收int字段的值。
     * 直方图被划分为一定数量的桶。
     * @param bucketsNum 桶的数量，用于划分输入的值
     * @param min 该直方图接收的最小值
     * @param max 该直方图接收的最大值
     */
    public IntHistogram(int bucketsNum, int min, int max) {
        this.bucketsNum = bucketsNum;
        this.min = min;
        this.max = max;
        this.width = (max - min) * 1.0 / bucketsNum;
        this.buckets = new int[bucketsNum];
        this.tuplesTotalNum = 0;
    }

    /**
     * 获取字段的值在该直方图中对应桶的索引
     * @param value 需要进行索引计算的值
     * @return 返回该值在直方图对应的桶的索引
     */
    private int getBucketIndex(int value) {
        if (value > max || value < min) {
            throw new IllegalArgumentException("The value is out of the histogram's range");
        }
        if (value == max) {
            // 注意最大值放在最后一个桶（左闭右闭）中，其它桶均是左闭右开
            return bucketsNum - 1;
        }
        return (int) ((value - min) / width);
    }

    /**
     * 将一个值添加到直方图中
     * @param value 需要添加到直方图的值
     */
    public void addValue(int value) {
        buckets[getBucketIndex(value)]++;
        tuplesTotalNum++;
    }

    /**
     * 通过指定的谓词和值来估计选择度（selectivity）。
     * 例：op为GREATER_THAN而value为5，返回表中大于5的值的比例的估计值
     * @param op 谓词操作符（GREATER_THAN、EQUAL等）
     * @param value 需要进行谓词比较的值
     * @return 返回估计的选择度（selectivity）
     */
    public double estimateSelectivity(Predicate.Op op, int value) {
        double selectivity = 0.0; // 选择度
        switch (op) {
            case LESS_THAN -> {
                if (value <= min) return 0.0; // 直方图中没有一个值小于value
                if (value >= max) return 1.0; // 直方图中所有值都小于value
                int bucketIndex = getBucketIndex(value);
                for (int i=0; i<bucketIndex; i++) {
                    selectivity += buckets[i] * 1.0 / tuplesTotalNum;
                }
                // value在自身所在的桶中所在的位置（%）
                double pos = (value - (bucketIndex * width + min)) / width;
                // 所在桶的元组数占元组总数量的比例（%）
                double ratio = buckets[bucketIndex] * 1.0 / tuplesTotalNum;
                selectivity += pos * ratio;
                return selectivity;
            }
            case LESS_THAN_OR_EQ -> {
                return estimateSelectivity(Predicate.Op.LESS_THAN, value + 1);
            }
            case GREATER_THAN -> {
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, value);
            }
            case GREATER_THAN_OR_EQ -> {
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, value);
            }
            case EQUALS -> {
                if (value < min || value > max) {
                    return 0.0;
                }
                // 所在桶的元组数占元组总数量的比例（%）
                double ratio = 1.0 * buckets[getBucketIndex(value)] / tuplesTotalNum;
                // width + 1 确保返回的selectivity在[0.0, 1.0]之间
                return ratio / ((int) width + 1);
            }
            case NOT_EQUALS -> {
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, value);
            }
        }
        return 0.0;
    }

    /**
     * @return 返回该直方图的平均选择度
     */
    public double avgSelectivity() {
        double avg = 0.0;
        for (int i=0; i<bucketsNum; i++) {
            avg += buckets[i] * 1.0 / tuplesTotalNum;
        }
        return avg;
    }

    /**
     * @return 返回IntHistogram的字符串描述
     */
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (int i=0; i<bucketsNum; i++) {
            double bucketLeft = i * width;
            double bucketRight = (i + 1) * width;
            // 打印每个桶的元组数量
            str.append(String.format("[%f, %f]:%d\n", bucketLeft, bucketRight, buckets[i]));
        }
        return str.toString();
    }
}
