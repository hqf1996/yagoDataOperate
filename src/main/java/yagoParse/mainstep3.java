package yagoParse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Util;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 15:57 2019/6/23
 * @Modified By:
 */
public class mainstep3 {
    public static String fs = "hdfs://10.1.13.111:8020";//hdfs://10.1.18.221:8020   psd:hdu52335335
    public static Logger LOG = LoggerFactory.getLogger(mainstep3.class);
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String currentDate = Util.getLocalData();
//        String pathHead = fs + "/user/freebase/result";
//        HdfsUtil.createDir(conf, pathHead, LOG);
        String []otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3){
            System.err.println("Usage:wordcount <in><out>");
            System.exit(3);
        }
//        conf.set("mapred.jar", "F:\\JavaProjects\\DataOperate\\target\\hadooptest2-1.0-SNAPSHOT-2.0.jar");
//        conf.set("mapreduce.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "yago_parse_step3");
        job.setJarByClass(step3.class);
        job.setMapperClass(step3.step3YagoMap1.class);
        job.setReducerClass(step3.step3YagoRuduce.class);

        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        job.addCacheFile(new URI(fs + "/user/freebase/freebase_links_en.ttl"));
//        job.addCacheFile(new URI(fs + "/user/freebase/test_map.ttl"));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
