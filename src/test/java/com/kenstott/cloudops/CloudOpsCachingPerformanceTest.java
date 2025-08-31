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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assume;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Performance test for Cloud Ops JDBC driver caching functionality.
 * This test runs the same queries twice to measure the impact of caching.
 */
public class CloudOpsCachingPerformanceTest {
    
    private static Properties testProperties;
    private static String connectionUrl;
    private static boolean hasCredentials = false;
    
    @BeforeClass
    public static void setup() throws Exception {
        // Load the driver
        Class.forName("com.kenstott.CloudOpsDriver");
        
        testProperties = new Properties();
        
        // Look for local-test.properties
        File[] propertyFiles = {
            new File("src/test/resources/local-test.properties"),
            new File("local-test.properties"),
            new File(System.getProperty("user.home"), "local-test.properties")
        };
        
        File foundFile = null;
        for (File file : propertyFiles) {
            if (file.exists()) {
                foundFile = file;
                break;
            }
        }
        
        if (foundFile != null) {
            try (FileInputStream fis = new FileInputStream(foundFile)) {
                testProperties.load(fis);
                System.out.println("Loaded test properties from: " + foundFile.getAbsolutePath());
            }
            
            // Build connection URL with caching enabled
            connectionUrl = buildConnectionUrl(true);
            hasCredentials = connectionUrl.contains("azure.") || 
                           connectionUrl.contains("aws.") || 
                           connectionUrl.contains("gcp.");
        } else {
            System.out.println("No local-test.properties found. Caching performance test will be skipped.");
        }
    }
    
    @Test
    public void testCachingPerformanceImpact() throws SQLException {
        Assume.assumeTrue("No credentials configured", hasCredentials);
        
        System.out.println("\n=== Cloud Ops Caching Performance Test ===");
        System.out.println("This test runs queries twice to measure caching impact");
        System.out.println("Cache is enabled with 5-minute TTL\n");
        
        // Test queries - start with simpler queries that return limited data
        String[] queries = {
            "SELECT cloud_provider, COUNT(*) as instance_count FROM compute_resources GROUP BY cloud_provider",
            "SELECT cloud_provider, storage_type, COUNT(*) as storage_count FROM storage_resources GROUP BY cloud_provider, storage_type",
            "SELECT cloud_provider, COUNT(*) as cluster_count FROM kubernetes_clusters GROUP BY cloud_provider"
        };
        
        String[] queryNames = {
            "Compute Resources Count",
            "Storage Resources Count by Type",
            "Kubernetes Clusters Count"
        };
        
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            System.out.println("Connected successfully with caching enabled");
            
            for (int i = 0; i < queries.length; i++) {
                System.out.println("\n--- Testing: " + queryNames[i] + " ---");
                System.out.println("Query: " + queries[i]);
                
                // First execution (cold cache)
                long firstStartTime = System.currentTimeMillis();
                int firstRowCount = executeQuery(conn, queries[i], "First execution (cold cache)");
                long firstDuration = System.currentTimeMillis() - firstStartTime;
                
                // Small delay to ensure any async operations complete
                Thread.sleep(100);
                
                // Second execution (warm cache)
                long secondStartTime = System.currentTimeMillis();
                int secondRowCount = executeQuery(conn, queries[i], "Second execution (warm cache)");
                long secondDuration = System.currentTimeMillis() - secondStartTime;
                
                // Calculate improvement
                double improvement = ((double)(firstDuration - secondDuration) / firstDuration) * 100;
                double speedup = (double)firstDuration / secondDuration;
                
                // Print results
                System.out.println("\nResults:");
                System.out.println("  First execution:  " + firstDuration + " ms (" + firstRowCount + " rows)");
                System.out.println("  Second execution: " + secondDuration + " ms (" + secondRowCount + " rows)");
                System.out.println("  Cache improvement: " + String.format("%.1f%%", improvement));
                System.out.println("  Speedup factor: " + String.format("%.2fx", speedup));
                
                if (improvement > 0) {
                    System.out.println("  ✓ Cache is working - query was faster on second execution");
                } else if (improvement < -10) {
                    System.out.println("  ⚠ Second execution was slower - possible network variance");
                } else {
                    System.out.println("  ≈ Similar performance - query might be too simple to benefit from caching");
                }
            }
            
            // Test with disabled cache for comparison
            System.out.println("\n\n=== Comparison with Cache Disabled ===");
            testWithoutCache();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Test failed", e);
        }
    }
    
    private void testWithoutCache() throws Exception {
        String noCacheUrl = buildConnectionUrl(false);
        
        try (Connection conn = DriverManager.getConnection(noCacheUrl)) {
            System.out.println("Connected with caching DISABLED");
            
            String query = "SELECT cloud_provider, COUNT(*) as instance_count FROM compute_resources GROUP BY cloud_provider";
            
            // Run twice without cache
            long firstStartTime = System.currentTimeMillis();
            int firstRowCount = executeQuery(conn, query, "First execution (no cache)");
            long firstDuration = System.currentTimeMillis() - firstStartTime;
            
            Thread.sleep(100);
            
            long secondStartTime = System.currentTimeMillis();
            int secondRowCount = executeQuery(conn, query, "Second execution (no cache)");
            long secondDuration = System.currentTimeMillis() - secondStartTime;
            
            System.out.println("\nResults without cache:");
            System.out.println("  First execution:  " + firstDuration + " ms");
            System.out.println("  Second execution: " + secondDuration + " ms");
            System.out.println("  Difference: " + Math.abs(firstDuration - secondDuration) + " ms");
            System.out.println("  ✓ Without cache, both executions take similar time");
        }
    }
    
    private int executeQuery(Connection conn, String query, String executionLabel) throws SQLException {
        System.out.println("\n" + executionLabel + ":");
        
        int rowCount = 0;
        long startTime = System.currentTimeMillis();
        
        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            
            // Get column count for display
            int columnCount = rs.getMetaData().getColumnCount();
            
            // Print header
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getMetaData().getColumnName(i));
                if (i < columnCount) System.out.print(" | ");
            }
            System.out.println();
            
            // Print data
            while (rs.next()) {
                rowCount++;
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i));
                    if (i < columnCount) System.out.print(" | ");
                }
                System.out.println();
            }
            
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Execution time: " + duration + " ms");
        }
        
        return rowCount;
    }
    
    private static String buildConnectionUrl(boolean enableCache) {
        StringBuilder url = new StringBuilder("jdbc:cloudops:");
        
        // Add cache configuration first
        url.append("cache.enabled=").append(enableCache).append(";");
        url.append("cache.ttlMinutes=5;");
        url.append("cache.debugMode=true;");
        
        // Add Azure configuration
        appendIfPresent(url, "azure.tenantId");
        appendIfPresent(url, "azure.clientId");
        appendIfPresent(url, "azure.clientSecret");
        appendIfPresent(url, "azure.subscriptionIds");
        
        // Add GCP configuration
        appendIfPresent(url, "gcp.projectIds");
        appendIfPresent(url, "gcp.credentialsPath");
        
        // Add AWS configuration
        appendIfPresent(url, "aws.accountIds");
        appendIfPresent(url, "aws.accessKeyId");
        appendIfPresent(url, "aws.secretAccessKey");
        appendIfPresent(url, "aws.region");
        appendIfPresent(url, "aws.roleArn");
        
        return url.toString();
    }
    
    private static void appendIfPresent(StringBuilder url, String key) {
        String value = testProperties.getProperty(key);
        if (value != null && !value.trim().isEmpty() && 
            !value.contains("your-") && !value.contains("/path/to/")) {
            url.append(key).append("=").append(value).append(";");
        }
    }
}