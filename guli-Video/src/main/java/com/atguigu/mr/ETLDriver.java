package com.atguigu.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ETLDriver implements Tool {

    private static Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        //获取job对象
        Job job = Job.getInstance(configuration);
        //设置jar包路径
        job.setJarByClass(ETLDriver.class);
        //设置Mapper类&输出KV类型
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出的KV类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交任务
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        //构建配置信息
        Configuration con = new Configuration();

        try {
            int run = ToolRunner.run(con, new ETLDriver(), args);
            System.out.println(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
