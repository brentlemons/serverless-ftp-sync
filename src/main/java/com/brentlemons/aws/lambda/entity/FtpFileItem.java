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
	
    static public class ZonedDateTimeConverter implements DynamoDBMarshaller<ZonedDateTime> {

        @Override
        public String marshall(ZonedDateTime time) {
            return time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

        @Override
        public ZonedDateTime unmarshall(Class<ZonedDateTime> dimensionType, String stringValue) {
            return ZonedDateTime.parse(stringValue);
        }
    }
}
