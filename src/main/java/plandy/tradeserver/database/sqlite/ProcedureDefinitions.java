package plandy.tradeserver.database.sqlite;

public class ProcedureDefinitions {
    public static final String I_LISTEDSTOCKS = "insert into LISTEDSTOCKS "
            + "(TICKER, "
            + "FULLNAME,"
            + "FLAGWATCHED) "
            + "values (?, ?, 0)";

    public static final String S_ALL_LISTEDSTOCKS = "select TICKER, "
            + "FULLNAME,"
            + "FLAGWATCHED "
            + "from LISTEDSTOCKS";

    public static final String U_WATCHLIST = "update LISTEDSTOCKS set FLAGWATCHED = ? where TICKER = ?";

    public static final String I_PRICEHISTORY = "insert or ignore into PRICEHISTORY "
            + "(TICKER, "
            + "DATE, "
            + "OPENPRICE, "
            + "HIGHPRICE, "
            + "LOWPRICE, "
            + "CLOSEPRICE, "
            + "VOLUME) "
            + "values (?,?,?,?,?,?,?) ";

    public static final String S_PRICEHISTORY = "select TICKER, "
            + "DATE, "
            + "OPENPRICE, "
            + "HIGHPRICE, "
            + "LOWPRICE, "
            + "CLOSEPRICE, "
            + "VOLUME "
            + "from PRICEHISTORY "
            + "where TICKER = ? "
            + "and date(DATE) > ? "
            + "order by DATE desc ";

    public static final String S_PRICEHISTORY_COUNT = "select count(*) as COUNT "
            + "from PRICEHISTORY "
            + "where TICKER = ? "
            + "and date(DATE) > ? "
            + "and date(DATE) < ? ";

    public static final String F_GET_MOSTRECENT_PRICEHISTORY_DATE = "select max(DATE) as DATE from PRICEHISTORY where TICKER = ? ";

    public static final String I_DATAREQUESTHISTORY = "insert into DATAREQUESTHISTORY "
            + "(TICKER, "
            + "REQUESTDATE) "
            + "values (?,?)";

    public static final String F_GET_MOSTRECENT_DATAREQUEST_DATE = "select max(REQUESTDATE) as REQUESTDATE from DATAREQUESTHISTORY where TICKER = ? ";

    public static final String F_IS_TABLE_EXISTS = "select 1 from SQLITE_MASTER where TYPE = 'table' and NAME = ? collate nocase";

}
