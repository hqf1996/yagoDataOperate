package yagoParse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 18:24 2019/6/23
 * @Modified By: 将step3和step4的所有实体或者关系的名称编号，为后面映射用，在此之前要先去重
 */
public class step5 {
    public static class step5YagoMap extends Mapper<Object, Text, Text, Text>{
        Text keyout = new Text();
        Text valueout = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String info[] = value.toString().split("\t");
            if (info.length == 1){
                keyout.set(info[0]);
                valueout.set(info[0]);
                context.write(keyout, valueout);
            }
        }
    }

    public static class step5YagoReduce extends Reducer<Text, Text, Text, NullWritable>{
        public int i = 0;
        Text keyout = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                keyout.set(value.toString() + "\t" + i);
                context.write(keyout, NullWritable.get());
                break;
            }
            i++;
        }
    }
}
