package mydb.optimizer;

import java.util.List;

/**
 * ���ڱ�ʾһ����ѯ�ƻ��ĳɱ���cost���ͻ�����cardinality��
 */
public class CostCardinality {

    /**
     * ���Ų�ѯ�ƻ��ĳɱ�
     */
    public double cost;

    /**
     * ���Ų�ѯ�ƻ��Ļ���
     */
    public int cardinality;

    /**
     * ���ŵĲ�ѯ�ƻ�
     */
    public List<LogicalJoinNode> plan;
}
