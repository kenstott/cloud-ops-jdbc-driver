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
import org.junit.Assume;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Tests for CloudOps query optimization features.
 * Verifies projection pushdown, filter pushdown, sorting, and pagination.
 */
public class CloudOpsQueryOptimizationTest {
    
    private Properties testProperties;
    private boolean hasCredentials = false;
    
    @Before
    public void setUp() throws IOException, ClassNotFoundException {
        Class.forName("com.kenstott.CloudOpsDriver");
        
        testProperties = new Properties();
        File propFile = new File("src/test/resources/local-test.properties");
        if (propFile.exists()) {
            try (FileInputStream fis = new FileInputStream(propFile)) {
                testProperties.load(fis);
                hasCredentials = testProperties.size() > 0;
            }
        }
    }
    
    private String buildConnectionUrl() {
        if (!hasCredentials) return "jdbc:cloudops:";
        
        StringBuilder url = new StringBuilder("jdbc:cloudops:");
        boolean first = true;
        for (String key : testProperties.stringPropertyNames()) {
            if (!first) url.append(";");
            url.append(key).append("=").append(testProperties.getProperty(key));
            first = false;
        }
        return url.toString();
    }
    
    @Test
    public void testProjectionPushdown() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query with specific column projection
            String query = "SELECT cloud_provider, instance_name, instance_type " +
                          "FROM cloudops.compute_resources LIMIT 5";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                ResultSetMetaData meta = rs.getMetaData();
                
                // Verify only requested columns are returned
                assertEquals("Should return exactly 3 columns", 3, meta.getColumnCount());
                assertEquals("First column should be cloud_provider", "cloud_provider", meta.getColumnName(1).toLowerCase());
                assertEquals("Second column should be instance_name", "instance_name", meta.getColumnName(2).toLowerCase());
                assertEquals("Third column should be instance_type", "instance_type", meta.getColumnName(3).toLowerCase());
                
                int count = 0;
                while (rs.next() && count < 5) {
                    assertNotNull("cloud_provider should not be null", rs.getString(1));
                    count++;
                }
                assertTrue("Should return at least one row", count > 0);
            }
        }
    }
    
    @Test
    public void testFilterPushdown() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query with filter condition
            String query = "SELECT resource_id, instance_name, cloud_provider " +
                          "FROM cloudops.compute_resources " +
                          "WHERE cloud_provider = 'azure' LIMIT 10";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    assertEquals("All results should be from Azure", "azure", provider.toLowerCase());
                }
            }
        }
    }
    
    @Test
    public void testComplexFilterConditions() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query with multiple filter conditions
            String query = "SELECT cluster_name, cloud_provider, node_count " +
                          "FROM cloudops.kubernetes_clusters " +
                          "WHERE node_count > 0 AND rbac_enabled = true " +
                          "ORDER BY node_count DESC LIMIT 10";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                int previousNodeCount = Integer.MAX_VALUE;
                while (rs.next()) {
                    int nodeCount = rs.getInt("node_count");
                    assertTrue("Node count should be greater than 0", nodeCount > 0);
                    assertTrue("Results should be sorted descending", nodeCount <= previousNodeCount);
                    previousNodeCount = nodeCount;
                }
            }
        }
    }
    
    @Test
    public void testSortingOptimization() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Test ascending sort
            String queryAsc = "SELECT resource_name, size_bytes " +
                             "FROM cloudops.storage_resources " +
                             "WHERE size_bytes IS NOT NULL " +
                             "ORDER BY size_bytes ASC LIMIT 5";
            
            try (ResultSet rs = stmt.executeQuery(queryAsc)) {
                long previousSize = 0;
                while (rs.next()) {
                    long size = rs.getLong("size_bytes");
                    assertTrue("Results should be sorted ascending", size >= previousSize);
                    previousSize = size;
                }
            }
            
            // Test descending sort
            String queryDesc = "SELECT resource_name, size_bytes " +
                              "FROM cloudops.storage_resources " +
                              "WHERE size_bytes IS NOT NULL " +
                              "ORDER BY size_bytes DESC LIMIT 5";
            
            try (ResultSet rs = stmt.executeQuery(queryDesc)) {
                long previousSize = Long.MAX_VALUE;
                while (rs.next()) {
                    long size = rs.getLong("size_bytes");
                    assertTrue("Results should be sorted descending", size <= previousSize);
                    previousSize = size;
                }
            }
        }
    }
    
    @Test
    public void testPaginationWithOffsetAndLimit() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Get first page
            String page1Query = "SELECT resource_id FROM cloudops.compute_resources " +
                               "ORDER BY resource_id LIMIT 5";
            
            Set<String> page1Ids = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery(page1Query)) {
                while (rs.next()) {
                    page1Ids.add(rs.getString(1));
                }
            }
            
            // Get second page with OFFSET
            String page2Query = "SELECT resource_id FROM cloudops.compute_resources " +
                               "ORDER BY resource_id LIMIT 5 OFFSET 5";
            
            Set<String> page2Ids = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery(page2Query)) {
                while (rs.next()) {
                    page2Ids.add(rs.getString(1));
                }
            }
            
            // Verify no overlap between pages
            for (String id : page2Ids) {
                assertFalse("Page 2 should not contain IDs from page 1", page1Ids.contains(id));
            }
        }
    }
    
    @Test
    public void testPreparedStatementOptimization() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            
            String query = "SELECT cluster_name, kubernetes_version FROM cloudops.kubernetes_clusters " +
                          "WHERE cloud_provider = ? LIMIT ?";
            
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                // Test with different parameter values
                pstmt.setString(1, "azure");
                pstmt.setInt(2, 3);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        assertNotNull("Cluster name should not be null", rs.getString(1));
                        count++;
                    }
                    assertTrue("Should return results", count > 0);
                    assertTrue("Should respect LIMIT", count <= 3);
                }
                
                // Reuse prepared statement with different parameters
                pstmt.setString(1, "aws");
                pstmt.setInt(2, 5);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertTrue("Should respect new LIMIT", count <= 5);
                }
            }
        }
    }
    
    @Test
    public void testAggregationPushdown() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Test COUNT aggregation
            String countQuery = "SELECT cloud_provider, COUNT(*) as resource_count " +
                               "FROM cloudops.compute_resources " +
                               "GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(countQuery)) {
                while (rs.next()) {
                    String provider = rs.getString(1);
                    int count = rs.getInt(2);
                    assertNotNull("Provider should not be null", provider);
                    assertTrue("Count should be positive", count >= 0);
                    System.out.println(provider + ": " + count + " resources");
                }
            }
            
            // Test with HAVING clause
            String havingQuery = "SELECT cloud_provider, COUNT(*) as resource_count " +
                                "FROM cloudops.storage_resources " +
                                "GROUP BY cloud_provider " +
                                "HAVING COUNT(*) > 0";
            
            try (ResultSet rs = stmt.executeQuery(havingQuery)) {
                while (rs.next()) {
                    int count = rs.getInt(2);
                    assertTrue("Count should be greater than 0 due to HAVING clause", count > 0);
                }
            }
        }
    }
    
    @Test
    public void testDistinctOptimization() throws SQLException {
        Assume.assumeTrue("Credentials required for optimization tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query for distinct cloud providers
            String query = "SELECT DISTINCT cloud_provider FROM cloudops.compute_resources";
            
            Set<String> providers = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    String provider = rs.getString(1);
                    assertFalse("Should not have duplicate providers", providers.contains(provider));
                    providers.add(provider);
                }
            }
            
            assertTrue("Should have at least one provider", providers.size() > 0);
            assertTrue("Should have at most 3 providers", providers.size() <= 3);
        }
    }
}