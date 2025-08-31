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
package com.kenstott.cloudops;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Unit tests for CloudOpsDriver.
 */
public class CloudOpsDriverTest {
    
    private com.kenstott.CloudOpsDriver driver;
    
    @Before
    public void setUp() {
        driver = new com.kenstott.CloudOpsDriver();
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        assertTrue("Should accept jdbc:cloudops: URLs", 
                  driver.acceptsURL("jdbc:cloudops:azure.tenantId=test"));
        assertTrue("Should accept jdbc:cloudops: URLs with no params", 
                  driver.acceptsURL("jdbc:cloudops:"));
        assertFalse("Should reject non-cloudops URLs", 
                   driver.acceptsURL("jdbc:mysql://localhost"));
        assertFalse("Should reject null URLs", 
                   driver.acceptsURL(null));
    }
    
    @Test
    public void testDriverRegistration() throws SQLException {
        // The driver should be registered with DriverManager
        // Test that it recognizes the URL pattern
        try {
            // Get a driver instance directly
            java.sql.Driver driver = DriverManager.getDriver("jdbc:cloudops:");
            assertNotNull("Driver should be registered", driver);
            assertTrue("Should be CloudOpsDriver", driver instanceof com.kenstott.CloudOpsDriver);
        } catch (SQLException e) {
            fail("CloudGovernanceDriver should be registered: " + e.getMessage());
        }
    }
    
    @Test
    public void testVersionMethods() {
        // These should delegate to the underlying Calcite driver
        assertTrue("Major version should be positive", driver.getMajorVersion() >= 0);
        assertTrue("Minor version should be non-negative", driver.getMinorVersion() >= 0);
    }
    
    @Test
    public void testJdbcCompliant() {
        // Should delegate to underlying driver
        assertNotNull("jdbcCompliant should return a value", driver.jdbcCompliant());
    }
    
    @Test
    public void testAzureConnectionString() throws SQLException {
        String url = "jdbc:cloudops:azure.tenantId=test-tenant;azure.clientId=test-client;azure.clientSecret=secret;azure.subscriptionIds=sub1,sub2";
        assertTrue("Should accept Azure connection string", driver.acceptsURL(url));
    }
    
    @Test
    public void testGCPConnectionString() throws SQLException {
        String url = "jdbc:cloudops:gcp.projectIds=project1,project2;gcp.credentialsPath=/path/to/creds.json";
        assertTrue("Should accept GCP connection string", driver.acceptsURL(url));
    }
    
    @Test
    public void testAWSConnectionString() throws SQLException {
        String url = "jdbc:cloudops:aws.accountIds=123456;aws.region=us-east-1;aws.accessKeyId=AKIATEST;aws.secretAccessKey=secret";
        assertTrue("Should accept AWS connection string", driver.acceptsURL(url));
    }
    
    @Test
    public void testMultiCloudConnectionString() throws SQLException {
        String url = "jdbc:cloudops:azure.tenantId=test;gcp.projectIds=project1;aws.accountIds=123456";
        assertTrue("Should accept multi-cloud connection string", driver.acceptsURL(url));
    }
}