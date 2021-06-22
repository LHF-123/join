package com.atguigu.reducejoin;

import com.atguigu.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/23 22:04
 */
public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();

    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //得到输入的数据的文件名
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (filename.equals("order.txt")){
            orderBean.setId(fields[0]);
            orderBean.setPid(fields[1]);
            orderBean.setAmount(Integer.parseInt(fields[2]));
            orderBean.setPname("");//order.txt里没有的要设置为空字符串
        } else {
            orderBean.setPid(fields[0]);
            orderBean.setPname(fields[1]);
            orderBean.setId("");
            orderBean.setAmount(0);
        }

        context.write(orderBean, NullWritable.get());
    }
}
