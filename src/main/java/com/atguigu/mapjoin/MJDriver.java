package com.atguigu.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @author LHF
 * @date 2020/12/24 12:05
 */
public class MJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MJDriver.class);

        job.setMapperClass(MJMapper.class);
        //ReduceTasks数量为0，不经过Reduce阶段
        job.setNumReduceTasks(0);

        //先把小表加载到内存中
        job.addCacheFile(URI.create("file:///F:/hadoopExer/MapJoin/input/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("F:\\hadoopExer\\MapJoin\\input\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\hadoopExer\\MapJoin\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
