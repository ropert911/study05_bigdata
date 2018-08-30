package common;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class TimeUtils {
    /**
     * 每秒的毫秒数
     */
    private static final int MILLSEC_PERSEC = 1000;
    /**
     * 时间转成数据类型格式 -- 分钟
     */
    private static final String TIMEFORMATE_MIN = "yyyyMMddHHmm";
    /**
     * 时间转成数据类型格式 -- 分钟
     */
    private static final String TIMEFORMATE_HOUR = "yyyyMMddHH";
    /**
     * 时间转成数据类型格式 -- 日
     */
    private static final String TIMEFORMATE_DAY = "yyyyMMdd";
    /**
     * 时间转成数据类型格式 -- 月
     */
    private static final String TIMEFORMATE_MONTH = "yyyyMM";

    /**
     * 把utc秒的时间转为分钟的本地时间格式 如1531280668=》20180711 1144
     *
     * @param utcSecs
     * @return
     */
    public static long convertToLocalMinFormat(long utcSecs) {
        SimpleDateFormat sf1 = new SimpleDateFormat(TIMEFORMATE_MIN);
        return Long.parseLong(sf1.format(new Date(utcSecs * MILLSEC_PERSEC)));
    }

    /**
     * 把utc秒的时间转为小时的本地时间格式 如1531280668=》20180711 1144
     *
     * @param utcSecs
     * @return
     */
    public static long convertToLocalHourFormat(long utcSecs) {
        SimpleDateFormat sf1 = new SimpleDateFormat(TIMEFORMATE_HOUR);
        return Long.parseLong(sf1.format(new Date(utcSecs * MILLSEC_PERSEC)));
    }

    /**
     * 把utc秒的时间转为分钟的本地时间格式 如1531280668=》20180711
     *
     * @param utcSecs
     * @return
     */
    public static long convertToLocalDayFormat(long utcSecs) {
        SimpleDateFormat sf1 = new SimpleDateFormat(TIMEFORMATE_DAY);
        return Long.parseLong(sf1.format(new Date(utcSecs * MILLSEC_PERSEC)));
    }
    /**
     * 把utc秒的时间转为分钟的本地时间格式 如1531280668=》201807
     *
     * @param utcSecs
     * @return
     */
    public static long convertToLocalMonthFormat(long utcSecs) {
        SimpleDateFormat sf1 = new SimpleDateFormat(TIMEFORMATE_MONTH);
        return Long.parseLong(sf1.format(new Date(utcSecs * MILLSEC_PERSEC)));
    }

    public static Calendar date2Calendar(Date date) {
        Calendar cal = null;
        if (date != null) {
            cal = GregorianCalendar.getInstance();
            cal.setTime(date);
        }
        return cal;
    }

    public static Calendar timestamp2Calendar(Timestamp timestamp) {
        Calendar cal = null;
        if (timestamp != null) {
            cal = GregorianCalendar.getInstance();
            cal.setTime(timestamp);
        }
        return cal;
    }

    public static final Timestamp getDefaultTimestamp() {
        return new Timestamp(-5364691200000L);
    }

    public static Calendar getCurrentDate() {
        return Calendar.getInstance();
    }

    public static Timestamp getCurrentTimestamp() {
        long time = System.currentTimeMillis();
        return new Timestamp(time);
    }

    public static Timestamp getCurrentTimestamp(String format) {
        if (format == null) {
            format = YYYY_MM_DD_FORMAT;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(getCurrentTime(format));
        } catch (ParseException e) {
            return null;
        }
        return new Timestamp(date.getTime());
    }

    public static String getCurrentTime(String parrten) {
        if (parrten == null || parrten.equals("")) {
            parrten = YYYY_MM_DD_FORMAT;
        }
        Date cday = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(parrten);
        String timestr = sdf.format(cday);
        return timestr;
    }

    public static String getCurrentTimeOfUs(String parrten) {
        if (parrten == null || parrten.equals("")) {
            parrten = YYYY_MM_DD_FORMAT;
        }
        Date cday = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(parrten, Locale.US);
        String timestr = sdf.format(cday);
        return timestr;
    }

    public static boolean isDefaultTimestamp(Timestamp time) {
        return time.getTime() == -5364691200000L;
    }

    public static boolean isDefaultTimestamp(Object time) {
        if (time instanceof Timestamp) {
            Timestamp timeValue = (Timestamp) time;
            return timeValue.getTime() == -5364691200000L;
        } else {
            return false;
        }
    }

    public static final Timestamp calendar2Timestamp(Calendar cal) {
        Date date = null;
        date = cal.getTime();
        return new Timestamp(date.getTime());
    }

    public static String timestamp2String(String format, Timestamp time) {
        if (getDefaultTimestamp().equals(time)) {
            return "";
        }
        if (format == null) {
            format = YYYY_MM_DD_FORMAT;
        }
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        if (time == null) {
            return null;
        } else {
            return formatter.format(time);
        }
    }

    public static final Timestamp string2Timestamp(String format, String time) {
        if (format == null) {
            format = YYYY_MM_DD_FORMAT;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            return null;
        }
        return new Timestamp(date.getTime());
    }

    public static final Date string2Date(String format, String time) {
        if (format == null) {
            format = YYYY_MM_DD_FORMAT;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
//        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneStr));
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            return null;
        }
        return date;
    }

    public static final Calendar string2Calendar(String format, String cal) {
        if (format == null) {
            format = YYYY_MM_DD_FORMAT;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(cal);
        } catch (ParseException e) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    public static final int getMonthDays(int year, int month) {
        switch (month) {
            case 1: // '\001'
                return 31;

            case 2: // '\002'
                return year % 4 != 0 || year % 100 == 0 && year % 400 != 0 ? 28 : 29;

            case 3: // '\003'
                return 31;

            case 4: // '\004'
                return 30;

            case 5: // '\005'
                return 31;

            case 6: // '\006'
                return 30;

            case 7: // '\007'
                return 31;

            case 8: // '\b'
                return 31;

            case 9: // '\t'
                return 30;

            case 10: // '\n'
                return 31;

            case 11: // '\013'
                return 30;

            case 12: // '\f'
                return 31;
        }
        return 0;
    }

    public static int getDaysDiff(String sdate1, String sdate2, String format, TimeZone tz) {
        SimpleDateFormat df = new SimpleDateFormat(format);
        Date date1 = null;
        Date date2 = null;
        try {
            date1 = df.parse(sdate1);
            date2 = df.parse(sdate2);
        } catch (ParseException pe) {
            return -1;
        }
        Calendar cal1 = null;
        Calendar cal2 = null;
        if (tz == null) {
            cal1 = Calendar.getInstance();
            cal2 = Calendar.getInstance();
        } else {
            cal1 = Calendar.getInstance(tz);
            cal2 = Calendar.getInstance(tz);
        }
        cal1.setTime(date1);
        long ldate1 = date1.getTime() + (long) cal1.get(15) + (long) cal1.get(16);
        cal2.setTime(date2);
        long ldate2 = date2.getTime() + (long) cal2.get(15) + (long) cal2.get(16);
        int hr1 = (int) (ldate1 / 3600000L);
        int hr2 = (int) (ldate2 / 3600000L);
        int days1 = hr1 / 24;
        int days2 = hr2 / 24;
        int dateDiff = days2 - days1;
        return dateDiff;
    }

    public static Calendar getWeek(int num) {
        Calendar date = Calendar.getInstance();
        int weekOfYear = date.get(3);
        weekOfYear += num;
        date.set(3, weekOfYear);
        return date;
    }

    public static Calendar getBeforeWeek(int num) {
        Calendar date = Calendar.getInstance();
        int weekOfYear = date.get(3);
        weekOfYear -= num;
        date.set(3, weekOfYear);
        return date;
    }

    public static Calendar getMonth(int num) {
        Calendar date = Calendar.getInstance();
        int monthOfYear = date.get(2);
        monthOfYear += num;
        date.set(2, monthOfYear);
        return date;
    }

    public static Calendar getBeforeMonth(int num) {
        Calendar date = Calendar.getInstance();
        int monthOfYear = date.get(2);
        monthOfYear += num;
        date.set(2, monthOfYear);
        return date;
    }

    public static Calendar getDay(int num) {
        Calendar date = Calendar.getInstance();
        int dayOfYear = date.get(5);
        dayOfYear += num;
        date.set(5, dayOfYear);
        return date;
    }

    public static Timestamp date2TimestampStart(Date date) {
        return new Timestamp(date.getTime());
    }

    public static String getDateString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat(YYYY_MM_DD_FORMAT);
        return format.format(date);
    }

    public static final String YYYY_MM_DD_FORMAT = "yyyy-MM-dd";
    public static final long DEFAULT_DATE = -5364691200000L;
    public static final String DEFAULTFORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String ELATICSEARCHFORMAT = "yyyy-MM-dd'T'HH:mm:ss+08:00";
    public static final String ELATICSEARCHFORMAT_NO8 = "yyyy-MM-dd'T'HH:mm:ss";

    public static final String CTSFORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    /**
     * 默认时区
     */
    public static final String DEFAULT_TIMEZONE = "GMT+08";


    public static String long2TimezoneStr(long time, String timeZoneStr, String farmat) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(farmat);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneStr));
        Date date = new Date(time);
        return dateFormat.format(date);
    }


    public static String long2TimezoneStr(long time, String timeZoneStr) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULTFORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneStr));
        Date date = new Date(time);
        return dateFormat.format(date);
    }

    public static String String2String(String time, String formatOld, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatOld);


        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dateFormat.format(date);
    }

    public static String String2StringUS(String time, String formatOld, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatOld, Locale.US);


        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dateFormat.format(date);
    }
}