package yagoParse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Util;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 19:59 2019/6/23
 * @Modified By: 将step1和step2的结果添加上id，并合并成一个文件，其中step5的结果为映射表
 */
public class step6 {
    public static class step6YagoMap extends Mapper<Object, Text, Text, NullWritable>{
        Text keyout = new Text();
        Map<String, String> yagoMap;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            yagoMap = Util.getMapFromDir("part-r-00000", "\t", 0, 1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String info[] = value.toString().split("\t");
            if (info.length == 3){
                // 在映射表中找到
                if (yagoMap.get(info[0])!=null && yagoMap.get(info[2])!=null){
                    keyout.set(yagoMap.get(info[0])+ "\t"+yagoMap.get(info[2])+"\t"+info[0]+"\t"+info[1]+"\t"+info[2]);
                    context.write(keyout, NullWritable.get());
                }
            }
        }
    }

}
