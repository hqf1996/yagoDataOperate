package yagoParse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 14:09 2019/6/24
 * @Modified By: 规整entity格式
 */
public class neo4jEntityFormat {
    public static class neo4jFormatMap extends Mapper<Object, Text, Text, NullWritable>{
        Text keyout = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString() != null){
                keyout.set(value.toString() + ",\"Entity\"");
                context.write(keyout, NullWritable.get());
            }
        }
    }
}
