/**
 * 
 */
package com.brentlemons.aws.lambda.entity;

import lombok.Data;

/**
 * @author brentlemons
 *
 */
@Data
public class FtpRequest {
	
	private String serviceName;
	private String host;
	private Integer port;
	private String username;
	private String password;
	private String path;
	private String filterExpression;

}
