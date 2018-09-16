/**
 * 
 */
package com.brentlemons.aws.lambda.entity;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import lombok.Data;

/**
 * @author brentlemons
 *
 */
@Data
@DynamoDBTable(tableName = "ftpFileList")
public class FtpFileItem {
	
    @DynamoDBHashKey(attributeName="fileKey")  
	private String fileKey;
    
    @DynamoDBRangeKey(attributeName="fileDate")
    @DynamoDBMarshalling(marshallerClass = ZonedDateTimeConverter.class)
    private ZonedDateTime fileDate;
    
	private String fileName;
    private String serviceName;
    private FtpRequest ftpRequest;
	
	/**
	 * 
	 */
	public FtpFileItem() {
		super();
	}

	/**
	 * @param fileKey
	 * @param fileDate
	 * @param fileName
	 * @param serviceName
	 * @param ftpRequest
	 */
	public FtpFileItem(String fileKey, ZonedDateTime fileDate, String fileName, String serviceName,
			FtpRequest ftpRequest) {
		super();
		this.fileKey = fileKey;
		this.fileDate = fileDate;
		this.fileName = fileName;
		this.serviceName = serviceName;
		this.ftpRequest = ftpRequest;
	}

	/**
	 * @param fileDate
	 * @param fileName
	 * @param serviceName
	 * @param ftpRequest
	 */
	public FtpFileItem(ZonedDateTime fileDate, String fileName, String serviceName,
			FtpRequest ftpRequest) {
		super();
		this.setFileKey(serviceName, fileName);
		this.fileDate = fileDate;
		this.fileName = fileName;
		this.serviceName = serviceName;
		this.ftpRequest = ftpRequest;
	}
	
	public void setFileKey(String serviceName, String fileName) {
		this.fileKey = serviceName + ":" + fileName;
	}

	static public class ZonedDateTimeConverter implements DynamoDBMarshaller<ZonedDateTime> {

        @Override
        public String marshall(ZonedDateTime time) {
            return time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

        // not using this for now, but if i do, i'll probably want to choose a format
        @Override
        public ZonedDateTime unmarshall(Class<ZonedDateTime> dimensionType, String stringValue) {
            return ZonedDateTime.parse(stringValue);
        }
    }

}
