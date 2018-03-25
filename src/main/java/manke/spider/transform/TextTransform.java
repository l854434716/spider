package manke.spider.transform;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by LENOVO on 2018/3/23.
 */
public class TextTransform {


    public  static  String   replaceNewLineSymbol(String  text,char c){

        return StringUtils.replaceChars(StringUtils.replaceChars(text,'\n',c),'\r',c);
    }

    public  static   String   replaceEnComma(String  text,char c){
        return StringUtils.replaceChars(text,',',c);
    }


    public  static   int   isSeasonFinish(String  update_info){

        if (StringUtils.contains(update_info,"更新")){
            return    0;
        }
        if (StringUtils.contains(update_info,"全")){
            return  1;
        }

        return   2;//未知
    }


    public  static  String   parsePlayCount(String  play_count){

        if (StringUtils.isNumeric(play_count)){
            return  play_count;
        }

        if (StringUtils.containsAny(play_count,'：',',')){

            play_count=TextTransform.removeTextComma(TextTransform.gainStrAfterChColon(play_count));
        }

        if (StringUtils.contains(play_count,"万")){

            return StringUtils.replace(play_count,"万","0000");
        }

        if (StringUtils.contains(play_count,"亿")){

            return StringUtils.replace(play_count,"亿","00000000");
        }

        return  "0";

    }


    public  static  String  removeTextComma(String  str){

        return  StringUtils.remove(str,',');
    }


    public   static   String  gainStrAfterChColon(String  str){

        return  StringUtils.substringAfter(str,"：");

    }
}
