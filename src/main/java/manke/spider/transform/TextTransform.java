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
}
