package fr.nic0w.trading.gdax;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class HistoryDownloader {

  private static final int PERIOD = 300;
  
  
  public static String toISO8601(Instant i) {
    
    return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX")
                .withZone(ZoneOffset.UTC)
                .format(i);
    
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    
    ObjectMapper jsonMapper = new ObjectMapper();

    Calendar calendar = Calendar.getInstance();
    
    calendar.set(2017, 10, 01, 12, 00);
    
    Instant stopDate = calendar.getTime().toInstant();
    
    String simpleDataUrlFormat= "https://api.gdax.com/products/ETH-EUR/candles?granularity=%d";
    
    Instant end = null, start=null;
    
    ArrayNode array = jsonMapper.createArrayNode();
    
    while(true) {
      
      String currentUrl = String.format(simpleDataUrlFormat, PERIOD);
      
      if(end != null && start != null) {
        
        currentUrl += "&start=" + toISO8601(start);
        currentUrl += "&end=" + toISO8601(end);
        
      }
      
      System.out.println(currentUrl);
      
      JsonNode data = jsonMapper.readTree(new URL(currentUrl));
      
      System.out.println("Got " + data.size() + " candles.");
      
      if(data.size() != 0) {
        
        long dataBeginTimestamp  = data.get(data.size()-1).get(0).asLong();
        
        end = Instant.ofEpochSecond(dataBeginTimestamp);
        
      }
      else {
        
        end = start;
        
      }
      
      start = end.minus(PERIOD * 350, ChronoUnit.SECONDS);

      
      array.addAll((ArrayNode) data); 
      
      if(end.isBefore(stopDate))
        break;
      
      Thread.sleep(500);
      
    }
    
    File out = new File("dataset.json");
    
    
    jsonMapper.writeValue(out, array);
    
    System.out.println("Done !");
  }

}
