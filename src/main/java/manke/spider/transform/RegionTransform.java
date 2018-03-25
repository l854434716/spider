package manke.spider.transform;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by luozhi on 2018/3/22.
 */
public class RegionTransform {

    private  static Map<String,String>  regionMap=new HashMap<>();


    static {
        regionMap.put("中国大陆","1");
        regionMap.put("内地","1");
        regionMap.put("大陆","1");
        regionMap.put("中国香港","2");
        regionMap.put("中国台湾","3");
        regionMap.put("台湾","3");
        regionMap.put("韩国","4");
        regionMap.put("英国","5");
        regionMap.put("美国","6");
        regionMap.put("法国","7");
        regionMap.put("日本","8");
        regionMap.put("德国","9");
        regionMap.put("加拿大","10");
        regionMap.put("其他","11");
        regionMap.put("其它地区","11");
        regionMap.put("芬兰","12");
        regionMap.put("捷克","13");
        regionMap.put("爱尔兰","14");
        regionMap.put("澳大利亚","15");
        regionMap.put("丹麦","16");
        regionMap.put("未知","17");
        regionMap.put("不详","17");

    }


    public  static   String  getRegionCodeByName(String name){

         if (regionMap.get(name)==null){
             return "17";
         }else{
             return regionMap.get(name);
         }
    }
}
