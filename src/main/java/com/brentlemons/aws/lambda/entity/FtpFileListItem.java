/**
 * 
 */
package com.brentlemons.aws.lambda.entity;

import java.time.ZonedDateTime;

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
public class FtpFileListItem {
	
    @DynamoDBHashKey(attributeName="fileName")  
	private String fileName;
    
    @DynamoDBRangeKey(attributeName="fileDate")
    @DynamoDBMarshalling(marshallerClass = ZonedDateTimeConverter.class)
    private ZonedDateTime fileDate;
    
    private String serviceName;
	
    static public class ZonedDateTimeConverter implements DynamoDBMarshaller<ZonedDateTime> {

        @Override
        public String marshall(ZonedDateTime time) {
            return time.toString();
        }

        @Override
        public ZonedDateTime unmarshall(Class<ZonedDateTime> dimensionType, String stringValue) {
            return ZonedDateTime.parse(stringValue);
        }
    }
}
