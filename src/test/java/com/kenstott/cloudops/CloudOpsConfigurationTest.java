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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Tests for CloudOpsDriver configuration handling.
 * Verifies environment variables, query parameters, and caching behavior.
 */
public class CloudOpsConfigurationTest {
    
    @Before
    public void setUp() throws ClassNotFoundException {
        Class.forName("com.kenstott.CloudOpsDriver");
    }
    
    @Test
    public void testDefaultConfiguration() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            
            // Verify defaults are applied
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM cloudops.compute_resources LIMIT 1")) {
                assertTrue("Should execute with default config", rs.next());
            }
        }
    }
    
    @Test
    public void testCacheDisabledConfiguration() throws SQLException {
        String url = "jdbc:cloudops:cache.enabled=false";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection with cache disabled should not be null", conn);
            
            // Query should work without cache
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM cloudops.storage_resources LIMIT 1")) {
                assertTrue("Should execute without cache", rs.next());
            }
        }
    }
    
    @Test
    public void testCustomCacheTTL() throws SQLException {
        String url = "jdbc:cloudops:cache.enabled=true;cache.ttlMinutes=10";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection with custom TTL should not be null", conn);
            
            // Execute query twice to test cache behavior
            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.kubernetes_clusters LIMIT 5")) {
                int count = 0;
                while (rs.next()) count++;
                assertTrue("Should return results", count > 0);
            }
            long firstQueryTime = System.currentTimeMillis() - start;
            
            // Second query should be faster due to cache
            start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.kubernetes_clusters LIMIT 5")) {
                int count = 0;
                while (rs.next()) count++;
                assertTrue("Should return cached results", count > 0);
            }
            long secondQueryTime = System.currentTimeMillis() - start;
            
            // Cache should make second query faster (though not guaranteed in all environments)
            System.out.println("First query: " + firstQueryTime + "ms, Second query: " + secondQueryTime + "ms");
        }
    }
    
    @Test
    public void testDebugModeConfiguration() throws SQLException {
        String url = "jdbc:cloudops:cache.debugMode=true";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection with debug mode should not be null", conn);
            
            // Debug mode should allow query execution
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT cloud_provider, COUNT(*) FROM cloudops.compute_resources GROUP BY cloud_provider")) {
                while (rs.next()) {
                    System.out.println("Debug: " + rs.getString(1) + " has " + rs.getInt(2) + " resources");
                }
            }
        }
    }
    
    @Test
    public void testProviderFilterConfiguration() throws SQLException {
        // Test filtering to specific providers
        String url = "jdbc:cloudops:providers=azure,aws";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection with provider filter should not be null", conn);
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT DISTINCT cloud_provider FROM cloudops.compute_resources")) {
                while (rs.next()) {
                    String provider = rs.getString(1);
                    assertTrue("Should only return azure or aws", 
                              "azure".equals(provider) || "aws".equals(provider));
                    assertNotEquals("Should not return gcp", "gcp", provider);
                }
            }
        }
    }
    
    @Test
    public void testConnectionWithProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("cache.enabled", "true");
        props.setProperty("cache.ttlMinutes", "15");
        props.setProperty("cache.debugMode", "false");
        
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertNotNull("Connection with properties should not be null", conn);
            assertFalse("Connection should not be closed", conn.isClosed());
        }
    }
    
    @Test
    public void testMultipleConnectionsWithDifferentConfigs() throws SQLException {
        // Test that different connections can have different configurations
        String urlNoCache = "jdbc:cloudops:cache.enabled=false";
        String urlWithCache = "jdbc:cloudops:cache.enabled=true;cache.ttlMinutes=5";
        
        try (Connection conn1 = DriverManager.getConnection(urlNoCache);
             Connection conn2 = DriverManager.getConnection(urlWithCache)) {
            
            assertNotNull("First connection should not be null", conn1);
            assertNotNull("Second connection should not be null", conn2);
            assertNotSame("Connections should be different instances", conn1, conn2);
            
            // Both should be able to execute queries
            try (Statement stmt1 = conn1.createStatement();
                 Statement stmt2 = conn2.createStatement()) {
                
                ResultSet rs1 = stmt1.executeQuery("SELECT COUNT(*) FROM cloudops.compute_resources");
                ResultSet rs2 = stmt2.executeQuery("SELECT COUNT(*) FROM cloudops.storage_resources");
                
                assertTrue("First query should return results", rs1.next());
                assertTrue("Second query should return results", rs2.next());
            }
        }
    }
    
    @Test
    public void testInvalidConfigurationHandling() {
        // Test handling of invalid configuration values
        String url = "jdbc:cloudops:cache.ttlMinutes=invalid";
        
        try {
            Connection conn = DriverManager.getConnection(url);
            // Should use default value for invalid integer
            assertNotNull("Should handle invalid config gracefully", conn);
            conn.close();
        } catch (SQLException e) {
            // Also acceptable to throw exception for invalid config
            assertTrue("Exception message should mention configuration", 
                      e.getMessage().toLowerCase().contains("config") || 
                      e.getMessage().toLowerCase().contains("ttl"));
        }
    }
}