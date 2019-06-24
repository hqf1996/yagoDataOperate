package yagoParse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 17:07 2019/6/23
 * @Modified By: 将step1和step2处理完的实体编号
 *                1.step1处理的是关系，step2处理的是实体
 *                2.将step1和step2同时读入，生成关联表
 *
 */
public class step3 {
    public static class step3YagoMap1 extends Mapper<Object, Text, Text, Text>{
        Text keyout = new Text();
        Text valueout = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info.length == 3){
                keyout.set(info[0]);
                valueout.set(info[0]);
                context.write(keyout, valueout);
            }
        }
    }

    public static class step3YagoRuduce extends Reducer<Text, Text, Text, NullWritable>{
        Text keyout = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                keyout.set(value.toString());
                context.write(keyout, NullWritable.get());
                break;
            }
        }
    }
}
