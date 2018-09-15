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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.brentlemons.aws.lambda.entity.FtpRequest;

import lombok.Data;

public class FtpSyncFunctionHandler implements RequestHandler<FtpRequest, String> {
	
	final FTPClient ftp;
	
	private ExecutorService ES;
    final AmazonDynamoDBAsync ddb;


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
    public String handleRequest(FtpRequest ftpRequest, Context context) {
		this.ES = Executors.newFixedThreadPool(100);
        context.getLogger().log("Input: " + ftpRequest);
        
//        String server = "tgftp.nws.noaa.gov";
//        String server = "ftp.ncep.noaa.gov";
        String remte = ftpRequest.getPath();
//        String remote = "/SL.us008001/DF.an/DC.sflnd/DS.metar/";
//        String remote = "/pub/data/nccf/com/rap/prod/rap.20180911";
        
        try {
            int reply;
            if (ftpRequest.getPort() == null)
            	ftp.connect(ftpRequest.getHost(), ftp.getDefaultPort());
            else
            	ftp.connect(ftpRequest.getHost(), ftpRequest.getPort());
            	
            context.getLogger().log("Connected to " + ftpRequest.getHost() + " on port " + (ftpRequest.getPort()!=null ? ftpRequest.getPort() : ftp.getDefaultPort()));

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
			if (!ftp.login(ftpRequest.getUsername(), ftpRequest.getPassword()))
			{
			    ftp.logout();
			}
			context.getLogger().log("Remote system is " + ftp.getSystemType());
			
            ftp.enterLocalPassiveMode();
            
            this.doConcurrent(ftp.listFiles(ftpRequest.getPath()), context, ftpRequest.getFilterExpression());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			context.getLogger().log("ioexception");
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			context.getLogger().log("interruptedexception");
			e.printStackTrace();
//		} catch (ExecutionException e) {
//			// TODO Auto-generated catch block
//			context.getLogger().log("executionexception");
//			e.printStackTrace();
		}



        // TODO: implement your handler
        return "Hello from Brent!";
    }

    public Object doConcurrent(final FTPFile[] files, Context context, String filterExpression) throws InterruptedException {
  	  context.getLogger().log("hello!");
  	  
    	final Collection<Callable<Future<PutItemResult>>> workers = new ArrayList<>();
        List<FTPFile> result = Arrays.stream(files).collect(Collectors.toList()).stream()
                .filter(s -> s.getName().matches(filterExpression))
                .collect(Collectors.toList());

		
		for (FTPFile f : result) {
			workers.add(new Task(f));
		}

			      final List<Future<Future<PutItemResult>>> jobs = ES.invokeAll(workers);
			      awaitTerminationAfterShutdown(ES);
			      for (final Future<Future<PutItemResult>> future : jobs) {
			         try {
						final PutItemResult r = future.get().get();
			         } catch (ConditionalCheckFailedException ex) {
			        	 context.getLogger().log("already there 0!");
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
			        	 context.getLogger().log("already there 1!");
					}
			      }
			      return null;
			   }

	   private final class Task implements Callable<Future<PutItemResult>>
	   {
	      private final FTPFile data;

	      Task(final FTPFile data) {
	         super();
	         this.data = data;
	      }

	      @Override
	      public Future<PutItemResult> call() throws Exception {
	      	  Map<String, AttributeValue> item = new HashMap<String,AttributeValue>();
	      	  item.put("serviceName", new AttributeValue().withS("brent"));
	    	  ZonedDateTime zdt = ZonedDateTime.ofInstant(data.getTimestamp().toInstant(), ZoneId.of("UTC"));
	      	  String hashString = "brent:" + 
	      			  data.getName() + ":" +
	      			  zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	      	  item.put("hashString", new AttributeValue().withS(hashString));
	      	  item.put("fileDate", new AttributeValue().withS(zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
	      	  item.put("fileName", new AttributeValue().withS(data.getName()));
	      	  
	      	  PutItemRequest putItemRequest = new PutItemRequest(
	      			  "ftpFileList", item);
	      	  putItemRequest.withConditionExpression("attribute_not_exists(fileName)");
	      	  
	      	  return ddb.putItemAsync(putItemRequest);
	      }
	   }
	   public void awaitTerminationAfterShutdown(ExecutorService threadPool) {
		    threadPool.shutdown();
		    try {
		        if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
		            threadPool.shutdownNow();
		        }
		    } catch (InterruptedException ex) {
		        threadPool.shutdownNow();
		        Thread.currentThread().interrupt();
		    }
		}
    
}
