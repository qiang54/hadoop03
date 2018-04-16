package com.hadoo.mr.invertedIndex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;

/**
 * 输入：(hello-->a.txt,{1,1,1,...})
 * 期望输出：(hello,a.txt-->3)
 */
public class invertedIndexStepTwo {

    public static class StepTwoMapper extends Mapper<LongWritable, Text, Text , Text>{

        //输入数据格式：(hello-->a.txt	3)

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] splits = StringUtils.split(line, "\t"); //  hello-->a.txt  3
            String[] wordAndName = StringUtils.split(splits[0], "-->"); //hello a.txt

            String word = wordAndName[0]; //hello
            String name = wordAndName[1];  //a.txt
            Long count = Long.parseLong(splits[1]);  //3

            //hello a.txt-->3
            context.write(new Text(word), new Text( name + "-->" + count));

        }
    }

    public static class StepTwoReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //输入数据:(hello ，{a.txt-->3,b.txt-->2,c.txt->1})
            String res = "";
            for (Text value: values
                 ) {
               res += value + ",";
            }

            context.write(key, new Text(res));
        }
    }


    public static  class StepTwoRunner extends Configured implements Tool{

        @Override
        public int run(String[] args) throws Exception {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(invertedIndexStepTwo.class);

            job.setMapperClass(StepTwoMapper.class);
            job.setReducerClass(StepTwoReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job,new Path(args[0]));

            //若输出路径存在，先删除
            Path output = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(output)){
                fs.delete(output, true);
            }


            FileOutputFormat.setOutputPath(job,output);

            return job.waitForCompletion(true) ? 0 : 1;

        }
    }

    public static void main(String[] args)  throws Exception {

        ToolRunner.run(new Configuration(), new StepTwoRunner() ,args);
    }
}
