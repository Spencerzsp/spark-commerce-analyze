package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    /**
     * 时间戳转换为时间
     * @param s
     * @return
     */
    public static String stampToDate(String s){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        String result = simpleDateFormat.format(date);
        return result;
    }

    /**
     * 时间转换为时间戳
     * @param s
     * @return
     * @throws ParseException
     */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }

    /**
     * 将字符串时间转换为date类型 Fri Jan 03 14:20:39 CST 2020
     * @param time
     * @return
     * @throws ParseException
     */
    public static Date parseTime(String time) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(time);

        return date;
    }

    /**
     * 将日期类型的时间转换为yyyy-MM-dd HH:mm:ss
     * @param date
     * @return
     */
    public static String formatTime(Date date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String result = simpleDateFormat.format(date);
        return result;
    }

    /**
     * 获取年月日小时
     * @param datetime
     * @return (yyyy-MM-dd_HH)
     */
    public static String getDateHour(String datetime){
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];

        return date + "_" + hour;
    }
}
