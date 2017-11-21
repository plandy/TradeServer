package plandy.tradeserver;

public class Stock {

    private final String ticker;
    private final String fullName;

    public Stock(String p_ticker, String p_fullName ) {
        ticker = p_ticker;
        fullName = p_fullName;
    }

    public String getTicker() {
        return ticker;
    }

    public String getFullName() {
        return fullName;
    }

}
