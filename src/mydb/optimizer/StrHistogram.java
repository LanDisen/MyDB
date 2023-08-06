package mydb.optimizer;

import mydb.execution.Predicate;

/**
 * 基于str类型字段的固定宽度直方图，本质是转换为IntHistogram间接实现
 */
public class StrHistogram {

    final IntHistogram intHistogram;

    final String minStr = "";
    final String maxStr = "zzzz";

    /**
     * StrHistogram构造函数，给定桶的数量创建一个直方图实例
     * @param bucketsNum 直方图中桶的数量
     */
    public StrHistogram(int bucketsNum) {
        intHistogram = new IntHistogram(bucketsNum, minValue(), maxValue());
    }

    /**
     * 将str转换为int，保证value(s1)<value(s2)，有s1<s2
     * 注意目前字符串最大长度为4（下标最大为3）
     * @param s 需要进行转换的字符串
     */
    private int strToInt(String s) {
        int i = 0;
        int value = 0;
        for (i=3; i>=0; i--) {
            if (s.length() > 3 - i) {
                int c = s.charAt(3 - i);
                value += (c) << (i * 8);
            }
        }
        // 确保字符串字段的值在直方图的范围内
        if (!(s.equals(minStr)) || s.equals(maxStr)) {
            int minVal = minValue();
            if (value < minVal) {
                value = minVal;
            }
            int maxVal = maxValue();
            if (value > maxVal) {
                value = maxVal;
            }
        }
        return value;
    }

    /**
     * @return 该直方图中能接收的字符串对应的最大值
     */
    int maxValue() {
        return strToInt(maxStr);
    }

    /**
     * @return 该直方图中能接收的字符串对应的最小值
     */
    int minValue() {
        return strToInt(minStr);
    }

    /**
     * 将一个字符串字段的值添加到该直方图中
     * @param str 需要添加到该直方图的字符串
     */
    public void addValue(String str) {
        this.intHistogram.addValue(strToInt(str));
    }

    /**
     * 给定谓词和字符串估计选择度
     * @param op 谓词操作符（Predicate.Op）
     * @param str 需要进行谓词比较的字符串
     * @return 估计的选择度
     */
    public double estimateSelectivity(Predicate.Op op, String str) {
        return intHistogram.estimateSelectivity(op, strToInt(str));
    }

    /**
     * @return 返回该直方图的平均选择度
     */
    public double avgSelectivity() {
        return this.intHistogram.avgSelectivity();
    }
}
