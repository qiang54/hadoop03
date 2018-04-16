package com.hadoo.mr.invertedIndex;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;


/**
 * 输入：原始数据
 * 期望输出：
 *
 */
public class InvertedIndexStepOne {

    public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //拿到一行数据
            String line = value.toString();
            String[] splits = StringUtils.split(line, " ");
            //获取文件名
            FileSplit filesplit = (FileSplit) context.getInputSplit();
            String fileName = filesplit.getPath().getName();

            for (String split: splits ) {
                //输出：K:hello-->a.txt v:1
                context.write(new Text(split + "-->" + fileName), new LongWritable(1));
            }

        }
    }

    public static class StepOneReducer extends Reducer<Text,LongWritable, Text,LongWritable >{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            //数据格式：(hello-->a.txt {1,1,1,...})
            //期望输出格式：hello-->a.txt	3
            Long count = 0L;
            
            //遍历values
            for (LongWritable value: values
                 )
                count += value.get();
            context.write(new Text(key), new LongWritable(count));
        }
    }

    public static class StepOneRunner extends Configured implements Tool {


        @Override
        public int run(String[] args) throws Exception {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            job.setJarByClass(InvertedIndexStepOne.class);

            job.setMapperClass(StepOneMapper.class);
            job.setReducerClass(StepOneReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);


            FileInputFormat.setInputPaths(job,new Path(args[0]) );
            FileOutputFormat.setOutputPath(job,new Path(args[1]) );


            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StepOneRunner(), args);
    }
}

