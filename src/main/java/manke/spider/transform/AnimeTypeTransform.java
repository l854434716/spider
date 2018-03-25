package manke.spider.transform;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by luozhi on 2018/3/22.
 */
public class AnimeTypeTransform {

    private  static Map<String,String> typeMap =new HashMap<>();


    static {
        typeMap.put("LOLI","1");
        typeMap.put("乙女","2");
        typeMap.put("亲子","3");
        typeMap.put("伪娘","4");
        typeMap.put("偶像","5");
        typeMap.put("催泪","6");
        typeMap.put("儿童历险","7");
        typeMap.put("儿童奇幻","8");
        typeMap.put("儿童搞笑","9");
        typeMap.put("儿童教育","10");
        typeMap.put("儿童益智","11");
        typeMap.put("其他","12");
        typeMap.put("冒险","13");
        typeMap.put("剧情","14");
        typeMap.put("动作","15");
        typeMap.put("励志","16");
        typeMap.put("历史","17");
        typeMap.put("原创","18");
        typeMap.put("合家欢","19");
        typeMap.put("同人","20");
        typeMap.put("后宫","21");
        typeMap.put("惊悚","22");
        typeMap.put("周边","23");
        typeMap.put("基腐","24");
        typeMap.put("声控","25");
        typeMap.put("奇幻","26");
        typeMap.put("少儿","27");
        typeMap.put("少儿经典","27");
        typeMap.put("青春","28");
        typeMap.put("少女","28");
        typeMap.put("少年","28");
        typeMap.put("忍者","29");
        typeMap.put("恋爱","30");
        typeMap.put("惊悚","31");
        typeMap.put("战争","32");
        typeMap.put("战斗","33");
        typeMap.put("推理","34");
        typeMap.put("搞笑","35");
        typeMap.put("教育","36");
        typeMap.put("日常","37");
        typeMap.put("时泪","38");
        typeMap.put("智斗","39");
        typeMap.put("机战","40");
        typeMap.put("校园","41");
        typeMap.put("格斗","42");
        typeMap.put("武侠","43");
        typeMap.put("治愈","44");
        typeMap.put("泡面","45");
        typeMap.put("游戏改","46");
        typeMap.put("漫改","47");
        typeMap.put("热血","48");
        typeMap.put("猎奇","49");
        typeMap.put("玄幻","50");
        typeMap.put("百合","51");
        typeMap.put("益智","52");
        typeMap.put("真人","53");
        typeMap.put("社会","54");
        typeMap.put("社团","55");
        typeMap.put("神话","56");
        typeMap.put("神魔","56");
        typeMap.put("科幻","57");
        typeMap.put("穿越","58");
        typeMap.put("竞技","59");
        typeMap.put("经典","60");
        typeMap.put("美少女","61");
        typeMap.put("美食","62");
        typeMap.put("职场","63");
        typeMap.put("致郁","64");
        typeMap.put("萌系","65");
        typeMap.put("装逼","66");
        typeMap.put("轻改","67");
        typeMap.put("运动","68");
        typeMap.put("青春","69");
        typeMap.put("音乐","70");
        typeMap.put("魔幻","71");
        typeMap.put("腾讯出品","72");
        typeMap.put("魔法","73");
        typeMap.put("未知","74");

    }


    public  static   String  getTypeCodeByName(String name){

         if (typeMap.get(name)==null){
             return "74";
         }else{
             return typeMap.get(name);
         }
    }
}
