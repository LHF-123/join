package com.atguigu.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.util.ReflectionUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author LHF
 * @date 2021/6/16 20:33
 */
public class TestCompress {
    public static void main(String[] args) throws IOException {
//        compress("F:\\hadoopExer\\ETL\\web.log", BZip2Codec.class);
        decompress("F:\\hadoopExer\\ETL\\web.log.bz2");
    }

    private static void compress(String path, Class<? extends CompressionCodec> codecClass) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, new Configuration());
        FileOutputStream fos = new FileOutputStream(path + codec.getDefaultExtension());

        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }

    private static void decompress(String path) throws IOException {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(path));

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(path));
        //去掉后缀，后缀长度不一定为4位
        FileOutputStream fos = new FileOutputStream(path.substring(0, path.length()-4));

        IOUtils.copyBytes(cis, fos, 1024);

        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }
}
