package com.atguigu.reducejoin;

import com.atguigu.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author LHF
 * @date 2020/12/23 22:41
 */
public class RJComparator extends WritableComparator {

    //Sort类通过继承WritableComparator来比较两个MyKey对象
    protected RJComparator() {
        super(OrderBean.class, true);
    }

    /**
     * @Description 将相同pid的放到一组KV值里处理，不然就会每一个OrderBean对象就是一个key
     *              bean里的compara是排序，这个是分组
     * @param
     * @throws
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
