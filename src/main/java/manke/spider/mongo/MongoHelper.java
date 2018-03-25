package manke.spider.mongo;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;

/**
 * Created by luozhi on 2018/3/25.
 */
public class MongoHelper {


    public   static  <T>  T getDocumentValue(Document  document,String key,Class<T> tClass){

        if (StringUtils.isEmpty(key)||document==null)
            return null;
        if (!StringUtils.contains(key,'.')&&!strHaveArraySymbol(key)){

            return   document.get(key,tClass);
        }


        String  _key=null;
        Document  _document=null;

        String  arrayKey=null;
        int   arrayIndex;
        ArrayList arrayList=null;
        if (StringUtils.contains(key,".")){

            _key=StringUtils.split(key,'.')[0];

            if (strHaveArraySymbol(_key)){// 数组情况
                arrayKey=StringUtils.substringBefore(_key,"[");
                arrayIndex=Integer.parseInt(StringUtils.substringBetween(_key,"[","]"));

                arrayList=document.get(arrayKey,ArrayList.class);

                if (arrayList!=null&&arrayList.size()>arrayIndex){
                    _document= (Document) arrayList.get(arrayIndex);
                }else{

                    _document=null;
                }

            }else{
                _document=document.get(_key,Document.class);
            }

            return  getDocumentValue(_document,StringUtils.substringAfter(key,"."),tClass);

        }else{//数组

            arrayKey=StringUtils.substringBefore(key,"[");
            arrayIndex=Integer.parseInt(StringUtils.substringBetween(key,"[","]"));

            arrayList=document.get(arrayKey,ArrayList.class);

            if (arrayList!=null&&arrayList.size()>arrayIndex){

                return (T) arrayList.get(arrayIndex);

            }else{

                return null;
            }


        }

    }



    public   static  <T>  T getDocumentValue(Document  document,String key,Class<T> tClass,T defultValue){

        T  t=getDocumentValue(document,key,tClass);
        if (t==null)
            return  defultValue;

        else
            return  t;

    }


    public  static  boolean    strHaveArraySymbol(String   str){

        if(StringUtils.countMatches(str,"[")==1&&StringUtils.countMatches(str,"]")==1
                &&StringUtils.isNumeric(StringUtils.substringBetween(str,"[","]"))
                &&StringUtils.isNotEmpty(StringUtils.substringBefore(str,"["))){
            return  true;
        }else{

            return  false;
        }
    }


    public static  void  main(String  arg[]){


        System.out.println(strHaveArraySymbol("1][[9]"));
    }
}
