package plandy.tradeserver;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtility {

    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static Date parseStringToDate( String p_dateString ) {

        Date l_parsedDate = null;

        DateFormat dateFormat = new SimpleDateFormat( DATE_FORMAT );
        try {
            l_parsedDate = dateFormat.parse( p_dateString );
        } catch (ParseException e) {
            throw new RuntimeException("error parsing date");
        }

        return l_parsedDate;
    }

    public static String parseDateToString ( Date p_date ) {
        String l_parsedDate = null;

        DateFormat dateFormat = new SimpleDateFormat( DATE_FORMAT );
        l_parsedDate = dateFormat.format( p_date );

        return l_parsedDate;
    }

    public static Date getTodayDate() {
        Date today = Calendar.getInstance().getTime();
        return today;
        //return Date.from( ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("GMT-1")).toInstant() );
    }

    public static Boolean isBeforeCalendarDate( Date p_date1, Date p_date2 ) {
        Boolean isBefore = false;

        Date truncatedDate1 = truncateTime( p_date1 );
        Date truncatedDate2 = truncateTime( p_date2 );

        if ( truncatedDate1.before(truncatedDate2) ) {
            isBefore = true;
        }

        return isBefore;
    }

    public static Boolean isSameCalendarDate( Date p_date1, Date p_date2 ) {
        Boolean isSame = false;

        Date truncatedDate1 = truncateTime( p_date1 );
        Date truncatedDate2 = truncateTime( p_date2 );

        if ( truncatedDate1.equals(truncatedDate2) ) {
            isSame = true;
        }

        return isSame;
    }

    public static Boolean isAfterCalendarDate( Date p_date1, Date p_date2 ) {
        Boolean isBefore = false;

        Date truncatedDate1 = truncateTime( p_date1 );
        Date truncatedDate2 = truncateTime( p_date2 );

        if ( truncatedDate1.after(truncatedDate2) ) {
            isBefore = true;
        }

        return isBefore;
    }

    public static Date truncateTime( Date p_date ) {
        Date truncatedDatetime = parseStringToDate( parseDateToString(p_date) );
        return truncatedDatetime;
    }

    public static Date addYears( Date p_date, int p_numYears ) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime( p_date );
        calendar.add( Calendar.YEAR, p_numYears );
        Date newDate = calendar.getTime();

        return newDate;
    }

    public static Date addDays( Date p_date, int p_numDays ) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime( p_date );
        calendar.add( Calendar.DATE, p_numDays );
        Date newDate = calendar.getTime();

        return newDate;
    }
}
