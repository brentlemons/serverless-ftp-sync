package com.brentlemons.aws.lambda;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.util.ImmutableMapParameter;
import com.brentlemons.aws.lambda.entity.FtpFileItem;
import com.brentlemons.aws.lambda.entity.FtpRequest;

import lombok.Data;

public class FtpSyncFunctionHandler implements RequestHandler<FtpRequest, String> {
		
	private final FTPClient ftp;
    private final AmazonDynamoDB ddb;
    private final DynamoDBSaveExpression saveExpression;
    private final DynamoDBMapper mapper;

	private ExecutorService ES;

    private final static String dynamoHashKey = "fileKey";
    private final static String dynamoRangeKey = "fileDate";

	/**
	 * 
	 */
	public FtpSyncFunctionHandler() {
		super();
		
		this.ftp = new FTPClient();
		this.ddb = new AmazonDynamoDBClient();
		ddb.setRegion(Region.getRegion(Regions.US_WEST_2));
		
		this.saveExpression = new DynamoDBSaveExpression().withExpected(ImmutableMapParameter.of(dynamoHashKey, new ExpectedAttributeValue(false)));
				
		DynamoDBMapperConfig mapperConfig = new DynamoDBMapperConfig(
				DynamoDBMapperConfig.SaveBehavior.CLOBBER);
			        
		this.mapper = new DynamoDBMapper(ddb, mapperConfig);

	}

    @Override
    public String handleRequest(FtpRequest ftpRequest, Context context) {
		this.ES = Executors.newFixedThreadPool(100);
        
        try {
            if (ftpRequest.getPort() == null)
            	ftp.connect(ftpRequest.getHost(), ftp.getDefaultPort());
            else
            	ftp.connect(ftpRequest.getHost(), ftpRequest.getPort());
            	
            context.getLogger().log("Connected to " + ftpRequest.getHost() + " on port " + (ftpRequest.getPort()!=null ? ftpRequest.getPort() : ftp.getDefaultPort()));

            // After connection attempt, you should check the reply code to verify
            // success.
            if (!FTPReply.isPositiveCompletion(ftp.getReplyCode())) {
                ftp.disconnect();
                context.getLogger().log("FTP server refused connection.");
                System.exit(1);
            }
		} catch (IOException e) {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                }
                catch (IOException f) {
                    // do nothing
                }
            }
            context.getLogger().log("Could not connect to server.");
            context.getLogger().log(e.getLocalizedMessage());
            System.exit(1);
		}
        
        try {
			if (!ftp.login(ftpRequest.getUsername(), ftpRequest.getPassword())) {
			    ftp.logout();
	            System.exit(1);
			}
			
            ftp.enterLocalPassiveMode();
            
            this.doConcurrent(ftp.listFiles(ftpRequest.getPath()), context, this.mapper, ftpRequest);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			context.getLogger().log(e.getLocalizedMessage());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			context.getLogger().log(e.getLocalizedMessage());
		}

        // TODO: implement your handler
        return "Hello from Brent!";
    }

    public Object doConcurrent(final FTPFile[] ftpFiles, Context context, DynamoDBMapper mapper, FtpRequest ftpRequest) throws InterruptedException {
  	  
    	final Collection<Callable<Result>> workers = new ArrayList<>();
        
    	List<FTPFile> files = Arrays.stream(ftpFiles).collect(Collectors.toList()).stream()
                .filter(s -> s.getName().matches(ftpRequest.getFilterExpression()))
                .collect(Collectors.toList());
		
		for (FTPFile file : files) {
			workers.add(new Task(file, ftpRequest, mapper));
		}

		final List<Future<Result>> jobs = ES.invokeAll(workers);
		
		awaitTerminationAfterShutdown(ES);
		
		for (final Future<Result> future : jobs) {
			try {
				final Result result = future.get();
				context.getLogger().log("INSERTED: " + result.getFileKey() + " | " + result.getFileDate().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			} catch (ConditionalCheckFailedException e) {
				// this is thrown if the record already exists and we told it not to allow insert on existing items
//				context.getLogger().log(e.getLocalizedMessage());
			} catch (ExecutionException e) {
				context.getLogger().log(e.getLocalizedMessage());
			}
		}
		return null;
	}

	private final class Task implements Callable<Result> {

		private final FTPFile data;
		private final FtpRequest ftpRequest;
		private final DynamoDBMapper mapper;

		Task(final FTPFile data, final FtpRequest ftpRequest, final DynamoDBMapper mapper) {
			super();
			this.data = data;
			this.ftpRequest = ftpRequest;
			this.mapper = mapper;
		}

		@Override
		public Result call() throws Exception {
			FtpFileItem item = new FtpFileItem(
					ZonedDateTime.ofInstant(this.data.getTimestamp().toInstant(), ZoneId.of("UTC")),
					this.data.getName(),
					this.ftpRequest.getServiceName(),
					ftpRequest);

			this.mapper.save(item, saveExpression);
	      	  
			return new Result(item.getFileKey(), item.getFileDate());
		}
	}
	   
	public void awaitTerminationAfterShutdown(ExecutorService threadPool) {
		
		threadPool.shutdown();
		
		try {
			if (!threadPool.awaitTermination(60, TimeUnit.SECONDS))
				threadPool.shutdownNow();
		} catch (InterruptedException ex) {
			threadPool.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
	   
	@Data
	private static final class Result {

		final String fileKey;
		final ZonedDateTime fileDate;
 
		Result(final String fileKey, final ZonedDateTime fileDate) {
			super();
			this.fileKey = fileKey;
			this.fileDate = fileDate;
		}
 
	}
    
}
