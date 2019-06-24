package yagoParse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.rdfRe;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 16:21 2019/6/23
 * @Modified By: 处理yagoSimpleTypes.ttl文件，并规整格式
 */
public class step2 {
    public static class step2YagoMap extends Mapper{
        Text keyout = new Text();
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info.length == 3 && info[1].equals("rdf:type")){
                String temp0 = rdfRe.ParseYago_step1(info[0]);
                String temp1 = "type";
                String temp2 = rdfRe.ParseYago_step1(info[2]);
                keyout.set(temp0 + "\t" + temp1 + "\t" + temp2);
                context.write(keyout, NullWritable.get());
            }
        }
    }
}
