package plandy.tradeserver.database.sqlite;

public class Tables {

    public static final String TABLE_LISTEDSTOCKS = "LISTEDSTOCKS";
    public static final String DROP_LISTEDSTOCKS = "drop table if exists LISTEDSTOCKS";
    public static final String CREATE_LISTEDSTOCKS = "create table LISTEDSTOCKS "
            + "(TICKER string not null, "
            + "FULLNAME string not null, "
            + "FLAGWATCHED boolean not null, "
            + "primary key (TICKER) )";

    public static final String TABLE_PRICEHISTORY = "PRICEHISTORY";
    public static final String DROP_PRICEHISTORY = "drop table if exists PRICEHISTORY ";
    public static final String CREATE_PRICEHISTORY = "create table PRICEHISTORY "
            + "(TICKER string not null, "
            + "DATE string not null, "
            + "OPENPRICE integer, "
            + "HIGHPRICE integer, "
            + "LOWPRICE integer, "
            + "CLOSEPRICE integer, "
            + "VOLUME integer, "
            + "primary key(TICKER, DATE) )";

    public static final String TABLE_DATAREQUESTHISTORY = "DATAREQUESTHISTORY";
    public static final String DROP_DATAREQUESTHISTORY = "drop table if exists DATAREQUESTHISTORY ";
    public static final String CREATE_DATAREQUESTHISTORY = "create table DATAREQUESTHISTORY "
            + "(TICKER string not null, "
            + "REQUESTDATE string not null, "
            + "primary key (TICKER, REQUESTDATE) )";

}
