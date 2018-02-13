package fr.nic0w.trading.gdax;

public enum Granularity {
    
  ONE_MINUTE(60), FIVE_MINUTES(300), FIFTEEN_MINUTES(900), ONE_HOUR(3600), SIX_HOUR(21600), ONE_DAY(86400);

  private final int candlePeriod;
  
  private Granularity(int period) {
    
    this.candlePeriod = period;
  }
  
  public int getCandlePeriod() {
    
    return this.candlePeriod;
  }  
}
