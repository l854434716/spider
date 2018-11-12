package manke.spider.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.jsoup.helper.StringUtil;

import java.util.regex.Pattern;

/**
 * Created by LENOVO on 2018/3/23.
 */
public class DateTransform {
    public   static  final   long   MILLSINONEDAY= 1000* 60 * 60 * 24;// 一天的毫秒值

    public   static  final   long   EIGHTHOURSMILLS= 28800000l; // 8个小时的毫秒值

    private  static Pattern patter1=Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");//yyyy-MM-dd HH:mm:ss

    public static   boolean  isPub_Time(String  pub_time){


        if (StringUtils.isEmpty(pub_time)){
            return  false;
        }else{
            return  patter1.matcher(pub_time).matches();
        }
    }


    public   static  long    getDayFirstTimeMills(long  timeMills){

        return   timeMills-(timeMills+EIGHTHOURSMILLS)%MILLSINONEDAY;
    }



    public  static  void  main(String  a[]){
        System.out.println(isPub_Time("2013-04-12 23:00:00"));
    }
}
