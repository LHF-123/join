package com.atguigu.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author LHF
 * @date 2020/12/24 13:01
 */
public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        //map阶段用BZip2压缩
//        configuration.set("mapreduce.map.output.compress", "true");
//        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);

//        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\hadoopExer\\ETL\\input"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\hadoopExer\\ETL\\output"));

        //设置reduce端输出压缩开启
//        FileOutputFormat.setCompressOutput(job, true);

        //设置压缩的方式
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
