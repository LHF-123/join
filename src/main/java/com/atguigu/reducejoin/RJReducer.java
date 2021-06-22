package com.atguigu.reducejoin;

import com.atguigu.bean.OrderBean;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author LHF
 * @date 2020/12/23 22:04
 */
public class RJReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //拿到迭代器
        Iterator<NullWritable> iterator = values.iterator();
        //数据指针下移，获取第一个OrderBean
        iterator.next();
        //从第一个OrderBean中取出品牌名称
        String pname = key.getPname();

        //遍历剩下的OrderBean，设置品牌名称并输出
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
