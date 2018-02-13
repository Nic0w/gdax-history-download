package fr.nic0w.trading.gdax;

import static joptsimple.util.DateConverter.datePattern;
import static joptsimple.util.RegexMatcher.regex;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.PathConverter;
import joptsimple.util.PathProperties;

public class HistoryDownloader implements Runnable {
  
  private final static String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mmX";

  private final static String GDAX_CANDLES_URL_FORMAT = "https://api.gdax.com/products/%s/candles?granularity=%d";

  private final ObjectMapper jsonMapper;
  
  private final Granularity granularity;

  private final String product;
  
  private Instant start, end;
  
  public HistoryDownloader(String product, Granularity granularity) {
    
    this.jsonMapper = new ObjectMapper();
    
    this.product = product;
    this.granularity = granularity;
    
  }
  
  public HistoryDownloader(String product, Granularity granularity, Instant start) {
    
    this(product, granularity);
    
    this.start = start;
  }
   
  public HistoryDownloader(String product, Granularity granularity, Instant start, Instant end) {
    
    this(product, granularity, start);
    
    this.end = end;
  }
  
  public static String toISO8601(Instant i) {
    
    return DateTimeFormatter.ofPattern(ISO8601_DATE_FORMAT)
                .withZone(ZoneOffset.UTC)
                .format(i);
    
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    
    OptionParser parser = new OptionParser();

    OptionSpec<String> productOption = parser.accepts("product", "Product. Ex: BTC-USD, ETH-EUR, ...").
        withRequiredArg().
        withValuesConvertedBy(regex("[A-Z]{3}-[A-Z]{3}")).required();
    
    OptionSpec<Granularity> granularityOption = parser.accepts("granularity", "Period of the candles.").
        withRequiredArg().
        ofType(Granularity.class).
        defaultsTo(Granularity.values()).
        required();
    
    OptionSpec<Date> startOption = parser.accepts("start", "From when to download historical data.").
        withRequiredArg().
        withValuesConvertedBy(datePattern(ISO8601_DATE_FORMAT));
    
    OptionSpec<Date> endOption = parser.accepts("end", "Until when to download historical data.").
        withRequiredArg().
        withValuesConvertedBy(datePattern(ISO8601_DATE_FORMAT));
    
    OptionSpec<Path> outputFileOption = parser.accepts("output", "Output file").
        withRequiredArg().
        withValuesConvertedBy(new PathConverter(PathProperties.WRITABLE));
    
    OptionSet parsedOptions = null;   
    try {
      
      parsedOptions = parser.parse(args);
      
    } catch (OptionException e) {
      
      
      parser.printHelpOn(System.out);
      
      System.exit(-1);
    }
    
    String product = parsedOptions.valueOf(productOption);
    
    Granularity granularity = parsedOptions.valueOf(granularityOption);
    
    Optional<Date> startDate = parsedOptions.valueOfOptional(startOption);
    
    Optional<Date> endDate = parsedOptions.valueOfOptional(endOption);

    
    HistoryDownloader downloader;
    
    if(startDate.isPresent() && endDate.isPresent()) {
      
      downloader = new HistoryDownloader(product, granularity, startDate.get().toInstant(), endDate.get().toInstant());
    }
    else if(startDate.isPresent()) {
      
      downloader = new HistoryDownloader(product, granularity, startDate.get().toInstant());      
    }
    else {
      
      downloader = new HistoryDownloader(product, granularity);      
    }
    
    downloader.run();
  }


  @Override
  public void run() {
    
    ArrayNode array = jsonMapper.createArrayNode();
    
    String baseURL = String.format(GDAX_CANDLES_URL_FORMAT, this.product, this.granularity.getCandlePeriod()); 
    
    Instant stop = start == null ? Instant.now() : start;
    
    do {
      
      String currentURL = baseURL;
      
      if(end != null && start != null) {
        
        currentURL += "&start=" + toISO8601(start);
        currentURL += "&end=" + toISO8601(end);
      }
      
      System.out.println("Requesting: " + currentURL);

      JsonNode data;
      
      try {
        data = jsonMapper.readTree(new URL(currentURL));
        
      } catch (IOException e) {
        
        System.err.println("I/O exception while fetching data from GDAX: " + e.getMessage());
        
        break;
      }
      
      System.out.println("Got " + data.size() + " candles.");
      
      if(data.size() != 0) {
        
        long dataBeginTimestamp  = data.get(data.size()-1).get(0).asLong();
        
        end = Instant.ofEpochSecond(dataBeginTimestamp);
        
      }
      else {
        
        end = start;
      }

      start = end.minus(this.granularity.getCandlePeriod() * 350, ChronoUnit.SECONDS);
      
      array.addAll((ArrayNode) data); 
      
      try {
        Thread.sleep(500);
        
      } catch (InterruptedException e) {
     
        System.err.println("Stopping: " + e.getMessage());
      }      
    }
    while(stop.isBefore(start));
    
    File out = new File(String.format("dataset_%s_%d_%d-%d.json", this.product, this.granularity.getCandlePeriod(), this.start.toEpochMilli(), this.end.toEpochMilli()));
    
    try {
      jsonMapper.writeValue(out, array);
    } catch (IOException e) {

      System.err.println("I/O exception while writing data to disk: " + e.getMessage());
    }
    
    System.out.println("Done !");    
  }

}
