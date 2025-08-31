/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kenstott;

import org.apache.calcite.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for cloud operations and multi-cloud resource management using Apache Calcite.
 * 
 * <p>This driver provides SQL access to cloud resources across multiple cloud providers
 * (Azure, GCP, AWS) for cloud operations, DevOps, SRE, and infrastructure management tasks.</p>
 * 
 * <p>Connection string format:</p>
 * <pre>
 * jdbc:cloudops:paramName=paramValue;paramName2=paramValue2
 * </pre>
 * 
 * <p>Example connection strings:</p>
 * <ul>
 * <li>Azure: jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1,sub2</li>
 * <li>GCP: jdbc:cloudops:gcp.projectIds=project1,project2;gcp.credentialsPath=/path/to/creds.json</li>
 * <li>AWS: jdbc:cloudops:aws.accountIds=123456;aws.region=us-east-1;aws.accessKeyId=xxx;aws.secretAccessKey=xxx</li>
 * <li>Multi-cloud: jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;gcp.projectIds=project1;gcp.credentialsPath=/path/to/creds.json</li>
 * </ul>
 * 
 * <p>Available parameters:</p>
 * <ul>
 * <li>Azure configuration:
 *   <ul>
 *     <li>azure.tenantId - Azure tenant ID</li>
 *     <li>azure.clientId - Azure client/application ID</li>
 *     <li>azure.clientSecret - Azure client secret</li>
 *     <li>azure.subscriptionIds - Comma-separated list of subscription IDs</li>
 *   </ul>
 * </li>
 * <li>GCP configuration:
 *   <ul>
 *     <li>gcp.projectIds - Comma-separated list of project IDs</li>
 *     <li>gcp.credentialsPath - Path to GCP credentials JSON file</li>
 *   </ul>
 * </li>
 * <li>AWS configuration:
 *   <ul>
 *     <li>aws.accountIds - Comma-separated list of account IDs</li>
 *     <li>aws.region - AWS region</li>
 *     <li>aws.accessKeyId - AWS access key ID</li>
 *     <li>aws.secretAccessKey - AWS secret access key</li>
 *     <li>aws.roleArn - Optional IAM role ARN for assume role</li>
 *   </ul>
 * </li>
 * </ul>
 */
public class CloudOpsDriver implements java.sql.Driver {
    private final Driver delegate;
    
    static {
        try {
            java.sql.DriverManager.registerDriver(new CloudOpsDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register CloudOpsDriver", e);
        }
    }
    
    public CloudOpsDriver() {
        this.delegate = new Driver();
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        String transformedUrl = transformUrl(url, info);
        return delegate.connect(transformedUrl, info);
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith("jdbc:cloudops:");
    }
    
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        String transformedUrl = transformUrl(url, info);
        return delegate.getPropertyInfo(transformedUrl, info);
    }
    
    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }
    
    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }
    
    @Override
    public boolean jdbcCompliant() {
        return delegate.jdbcCompliant();
    }
    
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }
    
    private String transformUrl(String cloudOpsUrl, Properties info) {
        // Extract parameters from the Cloud Ops URL
        String params = cloudOpsUrl.substring("jdbc:cloudops:".length());
        
        // Build the Calcite connection URL with Cloud Governance schema factory
        StringBuilder calciteUrl = new StringBuilder("jdbc:calcite:");
        
        // Build the model JSON inline - need to use 'inline:' prefix for inline JSON
        StringBuilder model = new StringBuilder();
        model.append("inline:{");
        model.append("\"version\":\"1.0\",");
        model.append("\"defaultSchema\":\"cloudops\",");
        model.append("\"schemas\":[{");
        model.append("\"name\":\"cloudops\",");
        model.append("\"type\":\"custom\",");
        model.append("\"factory\":\"org.apache.calcite.adapter.ops.CloudOpsSchemaFactory\",");
        model.append("\"operand\":{");
        
        // Parse the parameters and add them to the operand
        boolean first = true;
        if (!params.isEmpty()) {
            String[] paramPairs = params.split(";");
            for (String paramPair : paramPairs) {
                if (paramPair.contains("=")) {
                    String[] keyValue = paramPair.split("=", 2);
                    if (keyValue.length == 2) {
                        String key = keyValue[0].trim();
                        String value = keyValue[1].trim();
                        
                        if (!first) {
                            model.append(",");
                        }
                        model.append("\"").append(key).append("\":\"").append(escapeJson(value)).append("\"");
                        first = false;
                    }
                }
            }
        }
        
        model.append("}}]}");
        
        calciteUrl.append("model=").append(model);
        
        // Add additional Calcite properties for PostgreSQL compatibility
        calciteUrl.append(";lex=ORACLE");
        calciteUrl.append(";unquotedCasing=TO_LOWER");
        calciteUrl.append(";quoting=DOUBLE_QUOTE");
        calciteUrl.append(";caseSensitive=false");
        
        return calciteUrl.toString();
    }
    
    private String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                   .replace("\"", "\\\"")
                   .replace("\n", "\\n")
                   .replace("\r", "\\r")
                   .replace("\t", "\\t");
    }
}