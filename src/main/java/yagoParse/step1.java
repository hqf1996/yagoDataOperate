package yagoParse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.rdfRe;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 15:32 2019/6/23
 * @Modified By: 处理yagoFacts.ttl文件
    #@ <id_QuYEr?EEyC_H?S_97SE128hEA>
    <Robert_Murphy_Mayo>    <isLeaderOf>    <47th_Virginia_Infantry> .
    只提取出三元组，id去除，并规整格式

 */
public class step1 {
    public static class step1YagoMap extends Mapper<Object, Text, Text, NullWritable>{
        Text keyout = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info.length == 3){
                String temp0 = rdfRe.ParseYago_step1(info[0]);
                String temp1 = rdfRe.ParseYago_step1(info[1]);
                String temp2 = rdfRe.ParseYago_step1(info[2]);
                keyout.set(temp0 + "\t" + temp1 + "\t" + temp2);
                context.write(keyout, NullWritable.get());
            }
        }
    }
}
