package com.jinmiloan.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * 时间控制工具类
 * 
 *
 */
public class TimeUtil {
    public static final String DATE_FORMAT = "yyyyMMdd";
    
    // HBase表后缀，用于HBase按天分表
    public static final String HBASE_TABLE_NAME_SUFFIX_FORMAT = "yyyyMMdd";

    /**
     * 获取昨日的日期格式字符串数据
     * 
     * @return
     */
    public static String getYesterday() {
        return getYesterday(DATE_FORMAT);
    }

    /**
     * 获取对应格式的时间字符串
     * 
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 判断输入的参数是否是一个有效的时间格式数据
     * 
     * @param input
     * @return
     */
    public static boolean isValidateRunningDate(String input) {
        Matcher matcher = null;
        boolean result = false;
        String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
        if (input != null && !input.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(input);
        }
        if (matcher != null) {
            result = matcher.matches();
        }
        return result;
    }

    /**
     * 将yyyy-MM-dd格式的时间字符串转换为时间戳
     * 
     * @param input
     * @return
     */
    public static long parseString2Long(String input) {
        return parseString2Long(input, DATE_FORMAT);
    }

    /**
     * 将指定格式的时间字符串转换为时间戳
     * 
     * @param input
     * @param pattern
     * @return
     */
    public static long parseString2Long(String input, String pattern) {
        Date date = null;
        try {
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }

    /**
     * 将时间戳转换为yyyy-MM-dd格式的时间字符串
     * 
     * @param input
     * @return
     */
    public static String parseLong2String(long input) {
        return parseLong2String(input, DATE_FORMAT);
    }

    /**
     * 将时间戳转换为指定格式的字符串
     * 
     * @param input
     * @param pattern
     * @return
     */
    public static String parseLong2String(long input, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /**
     * 将nginx服务器时间转换为时间戳，如果说解析失败，返回-1
     * 
     * @param input
     * @return
     */
    public static long parseNginxServerTime2Long(String input) {
        Date date = parseNginxServerTime2Date(input);
        return date == null ? -1L : date.getTime();
    }

    /**
     * 将nginx服务器时间转换为date对象。如果解析失败，返回null
     * 
     * @param input
     *            格式: 1449410796.976
     * @return
     */
    public static Date parseNginxServerTime2Date(String input) {
        if (StringUtils.isNotBlank(input)) {
            try {
                long timestamp = Double.valueOf(Double.valueOf(input.trim()) * 1000).longValue();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(timestamp);
                return calendar.getTime();
            } catch (Exception e) {
                // nothing
            }
        }
        return null;
    }

   

    /**
     * 获取time指定周的第一天的时间戳值
     * 
     * @param time
     * @return
     */
    public static long getFirstDayOfThisWeek(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * @param date
     *            : long date like "1360252800000L"
     * @return : long of first day of this month
     */
    public static long getFirstDayOfThisMonth(long date) {
        Date d = new Date();
        d.setTime(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取给定时间上一个月的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfPreviousMonth(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.MONDAY, -1); // 月份减少1
        cal.set(Calendar.DAY_OF_MONTH, 1); // 天设置为月的第一天
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取下个月的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfNextMonth(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.MONDAY, 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取上一周的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfPreviousWeek(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.WEEK_OF_YEAR, -1); // 周数减少1
        cal.set(Calendar.DAY_OF_WEEK, 1); // 设置获取第一天
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取下一周的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfNextWeek(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.WEEK_OF_YEAR, 1);
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取当天的毫米及
     * 
     * @return
     */
    public static long getTodayInMillis() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * @param amount
     *            : amount day before or after.
     * @param today
     *            : long date like 1360252800000L
     * @return : string of long date like 1360252800000 <br />
     *         example : getSpecifiedDate (today, 2) : get the date of the day
     *         after tomorrow<br />
     *         getSpecifiedDate (today, -1) : get the date of yesterday
     */
    public static long getSpecifiedDate(long today, int amount) {
        Date d = new Date();
        d.setTime(today);
        Calendar calStart = Calendar.getInstance();
        calStart.setTime(d);
        calStart.add(Calendar.DAY_OF_YEAR, amount);
        calStart.set(Calendar.HOUR_OF_DAY, 0);
        calStart.set(Calendar.MINUTE, 0);
        calStart.set(Calendar.SECOND, 0);
        calStart.set(Calendar.MILLISECOND, 0);
        return calStart.getTimeInMillis();
    }
}
