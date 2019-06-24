import java.util.HashMap;
import java.util.Map;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 20:23 2019/6/23
 * @Modified By:
 */
public class test {
    public static void main(String[] args) {
        Map<String, String> mymap = new HashMap<>();
        mymap.put("aa", "1");
        mymap.put("bb", "2");
        if (mymap.get("aa") != null){
            System.out.println("111");
        }

    }


}
