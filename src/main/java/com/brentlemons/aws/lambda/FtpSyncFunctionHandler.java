package com.brentlemons.aws.lambda;

import java.io.IOException;
import java.net.SocketException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class FtpSyncFunctionHandler implements RequestHandler<Object, String> {
	
	final FTPClient ftp;
	
	private static final ExecutorService ES = Executors.newCachedThreadPool();
    final AmazonDynamoDB ddb;


	/**
	 * 
	 */
	public FtpSyncFunctionHandler() {
		super();
		
		this.ftp = new FTPClient();
//		AWSCredentials credentials;
		this.ddb = new AmazonDynamoDBAsyncClient();
		ddb.setRegion(Region.getRegion(Regions.US_WEST_2));
	}

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        
        String server = "tgftp.nws.noaa.gov";
//        String server = "ftp.ncep.noaa.gov";
        String remote = "/SL.us008001/DF.an/DC.sflnd/DS.metar/";
//        String remote = "/pub/data/nccf/com/rap/prod/rap.20180911";
        
        try {
            int reply;
			ftp.connect(server);
			context.getLogger().log("Connected to " + server);// + " on " + (port>0 ? port : ftp.getDefaultPort()));

            // After connection attempt, you should check the reply code to verify
            // success.
            reply = ftp.getReplyCode();

            if (!FTPReply.isPositiveCompletion(reply))
            {
                ftp.disconnect();
                context.getLogger().log("FTP server refused connection.");
                System.exit(1);
            }
		} catch (IOException e) {
            if (ftp.isConnected())
            {
                try
                {
                    ftp.disconnect();
                }
                catch (IOException f)
                {
                    // do nothing
                }
            }
            context.getLogger().log("Could not connect to server.");
            e.printStackTrace();
            System.exit(1);
		}
        
        try {
			if (!ftp.login("anonymous", "brent@home.com"))
			{
			    ftp.logout();
			}
			context.getLogger().log("Remote system is " + ftp.getSystemType());
			
            ftp.enterLocalPassiveMode();
            
            this.doConcurrent(ftp.listFiles(remote), context);

//                for (FTPFile f : ftp.listFiles(remote)) {
////                	context.getLogger().log(f.getName());//.getRawListing());
//                    ZonedDateTime zdt = ZonedDateTime.ofInstant(f.getTimestamp().toInstant(), ZoneId.of("UTC"));
////                    context.getLogger().log(zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
//                }


		} catch (IOException e) {
			// TODO Auto-generated catch block
			context.getLogger().log("ioexception");
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			context.getLogger().log("interruptedexception");
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			context.getLogger().log("executionexception");
			e.printStackTrace();
		}



        // TODO: implement your handler
        return "Hello from Brent!";
    }

    public List<Result> doConcurrent(final FTPFile[] files, Context context)
    		throws InterruptedException, ExecutionException {
  	  context.getLogger().log("hello!");
  	  
    	final Collection<Callable<Result>> workers = new ArrayList<>();
		int ii = 0;
        List<FTPFile> result = Arrays.stream(files).collect(Collectors.toList()).stream()                // convert list to stream
                .filter(s -> s.getName().matches("sn\\.[0-9]{4}\\.txt$"))     // we dont like mkyong
//                .filter(s -> s.getName().matches("rap\\.t[0-9]{2}z\\.awp130pgrbf[0-9]{2}\\.grib2$"))     // we dont like mkyong
                .collect(Collectors.toList());              // collect the output and convert streams to a List

		
		for (FTPFile f : result) {
			workers.add(new Task(ii++, f, context));
		}

			      final List<Future<Result>> jobs = ES.invokeAll(workers);
//			      final FeatureCollection features = new FeatureCollection();
			      List<Result> results = new ArrayList<Result>();
			      for (final Future<Result> future : jobs) {
			         final Result r = future.get();
			         results.add(future.get());
//			         MultiPolygon path = (MultiPolygon)r.path.getGeometry();
//			         if (path.getCoordinates().size() > 0)
//			        	 features.add(r.path);
			      }
			      return results;
			   }

	   private final class Task implements Callable<Result>
	   {
	      private final int ndx;
	      private final FTPFile data;
	      private final Context context;

	      Task(final int ndx, final FTPFile data, final Context context) {
	         super();
	         this.ndx = ndx;
	         this.data = data;
	         this.context = context;
	      }

	      @Override
	      public Result call() throws Exception {
	    	  context.getLogger().log("index: " + ndx + " | file: " + data.getName());
	      	  Map<String, AttributeValue> item = new HashMap<String,AttributeValue>();
	      	  item.put("serviceName", new AttributeValue().withS("brent"));
	    	  ZonedDateTime zdt = ZonedDateTime.ofInstant(data.getTimestamp().toInstant(), ZoneId.of("UTC"));
	      	  String hashString = "brent:" + 
	      			  data.getName() + ":" +
	      			  zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	      	  item.put("fileHash", new AttributeValue().withN(String.valueOf(hashString.hashCode())));
	      	  item.put("fileDate", new AttributeValue().withS(zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
	      	  item.put("fileName", new AttributeValue().withS(data.getName()));
	      	  
	      	  ddb.putItem("ftpFileList", item);
	      	

	    	  //            ZonedDateTime zdt = ZonedDateTime.ofInstant(f.getTimestamp().toInstant(), ZoneId.of("UTC"));
////          context.getLogger().log(zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
	         return new Result(ndx, data.getName());
	      }
	   }

	   
	   private static final class Result
	   {
	      final int ndx;
	      final String path;
	      private transient String str;


	      Result(final int ndx, final String path) {
	         super();
	         this.ndx = ndx;
	         this.path = path;
	      }

	      @Override
	      public String toString() {
	         if (str == null) {
	            str = new StringBuilder("Result{ndx=").append(ndx)
	                  .append('}')
	                  .toString();
	         }
	         return str;
	      }
	   }
	   

    
}
