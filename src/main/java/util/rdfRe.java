package util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 15:42 2019/6/23
 * @Modified By: 正则表达式匹配内容
 */
public class rdfRe {
    public static String ParseYago_step1(String input){
        String output = "";
        String pattern = "<(.*?)>";
        Pattern p = Pattern.compile(pattern);

        Matcher m = p.matcher(input);
        if (m.find()){
            output = m.group(1);
        }else{
            output = input;
        }
        return output;
    }

    public static void main(String[] args) {
        String a = "<Robert_Murphy_Mayo>" + "\t"+ "<isLeaderOf>" +"\t"+ "<47th_Virginia_Infantry> .";
        String []split = a.split("\t");
        for (int i = 0 ; i < split.length ; ++i){
            System.out.println(ParseYago_step1(split[i]));
        }

    }
}
