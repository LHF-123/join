package com.atguigu.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LHF
 * @date 2020/12/24 12:05
 */
public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<>();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] field = line.split("\t");
            pMap.put(field[0], field[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //这里的fields[1]是pid，根据pid在Map里得到对应的pname，Map中pid是Key
        String pname = pMap.get(fields[1]);
        if (pname == null) {
            pname = "NULL";
        }
        k.set(fields[0] + "\t" + pname + "\t" + fields[2]);
        context.write(k, NullWritable.get());
    }
}
