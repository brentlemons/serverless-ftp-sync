/**
 * 
 */
package com.brentlemons.aws.lambda.entity;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;

import lombok.Data;

/**
 * @author brentlemons
 *
 */
@Data
@DynamoDBDocument
public class FtpRequest {
	
	private String serviceName;
	private String host;
	private Integer port;
	private String username;
	private String password;
	private String path;
	private String filterExpression;

}
