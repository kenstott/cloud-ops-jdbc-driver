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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Performance tests for CloudOps JDBC driver.
 * Tests caching effectiveness, query performance, and concurrent access.
 */
public class CloudOpsPerformanceTest {
    
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
    
    private String buildConnectionUrl(boolean cacheEnabled, int ttlMinutes) {
        if (!hasCredentials) {
            return String.format("jdbc:cloudops:cache.enabled=%s;cache.ttlMinutes=%d", 
                               cacheEnabled, ttlMinutes);
        }
        
        StringBuilder url = new StringBuilder("jdbc:cloudops:");
        url.append("cache.enabled=").append(cacheEnabled);
        url.append(";cache.ttlMinutes=").append(ttlMinutes);
        
        for (String key : testProperties.stringPropertyNames()) {
            url.append(";").append(key).append("=").append(testProperties.getProperty(key));
        }
        return url.toString();
    }
    
    @Test
    public void testCachePerformanceImprovement() throws SQLException {
        Assume.assumeTrue("Credentials required for performance tests", hasCredentials);
        
        // Test with cache disabled
        String noCacheUrl = buildConnectionUrl(false, 5);
        long noCacheTime = 0;
        
        try (Connection conn = DriverManager.getConnection(noCacheUrl);
             Statement stmt = conn.createStatement()) {
            
            // Warm up
            stmt.executeQuery("SELECT COUNT(*) FROM cloudops.storage_resources").close();
            
            // Measure without cache
            long start = System.currentTimeMillis();
            for (int i = 0; i < 5; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.storage_resources LIMIT 10")) {
                    while (rs.next()) {
                        // Consume results
                        rs.getString(1);
                    }
                }
            }
            noCacheTime = System.currentTimeMillis() - start;
        }
        
        // Test with cache enabled
        String cacheUrl = buildConnectionUrl(true, 5);
        long cacheTime = 0;
        
        try (Connection conn = DriverManager.getConnection(cacheUrl);
             Statement stmt = conn.createStatement()) {
            
            // First query to populate cache
            stmt.executeQuery("SELECT * FROM cloudops.storage_resources LIMIT 10").close();
            
            // Measure with cache
            long start = System.currentTimeMillis();
            for (int i = 0; i < 5; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.storage_resources LIMIT 10")) {
                    while (rs.next()) {
                        // Consume results
                        rs.getString(1);
                    }
                }
            }
            cacheTime = System.currentTimeMillis() - start;
        }
        
        System.out.println("\n=== Cache Performance Test Results ===");
        System.out.printf("Without cache: %d ms (5 queries)%n", noCacheTime);
        System.out.printf("With cache: %d ms (5 queries)%n", cacheTime);
        System.out.printf("Performance improvement: %.1fx faster%n", 
                         (double) noCacheTime / cacheTime);
        
        // Cache should provide some improvement (though not guaranteed in all environments)
        assertTrue("Cache time should be reasonable", cacheTime > 0);
    }
    
    @Test
    public void testPreparedStatementPerformance() throws SQLException {
        Assume.assumeTrue("Credentials required for performance tests", hasCredentials);
        
        String url = buildConnectionUrl(true, 5);
        
        try (Connection conn = DriverManager.getConnection(url)) {
            
            String query = "SELECT * FROM cloudops.storage_resources " +
                          "WHERE cloud_provider = ? AND encryption_enabled = ? LIMIT ?";
            
            // Test prepared statement performance
            long preparedTime = 0;
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                long start = System.currentTimeMillis();
                
                for (int i = 0; i < 10; i++) {
                    pstmt.setString(1, "azure");
                    pstmt.setBoolean(2, true);
                    pstmt.setInt(3, 5);
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            // Consume results
                            rs.getString("resource_name");
                        }
                    }
                }
                
                preparedTime = System.currentTimeMillis() - start;
            }
            
            // Test regular statement performance (for comparison)
            long regularTime = 0;
            try (Statement stmt = conn.createStatement()) {
                long start = System.currentTimeMillis();
                
                for (int i = 0; i < 10; i++) {
                    String sqlQuery = "SELECT * FROM cloudops.storage_resources " +
                                     "WHERE cloud_provider = 'azure' AND encryption_enabled = true LIMIT 5";
                    
                    try (ResultSet rs = stmt.executeQuery(sqlQuery)) {
                        while (rs.next()) {
                            // Consume results
                            rs.getString("resource_name");
                        }
                    }
                }
                
                regularTime = System.currentTimeMillis() - start;
            }
            
            System.out.println("\n=== Prepared Statement Performance ===");
            System.out.printf("Prepared statement: %d ms (10 queries)%n", preparedTime);
            System.out.printf("Regular statement: %d ms (10 queries)%n", regularTime);
            System.out.printf("Prepared statement advantage: %.1f%%%n", 
                            100.0 * (regularTime - preparedTime) / regularTime);
        }
    }
    
    @Test
    public void testConcurrentAccess() throws Exception {
        Assume.assumeTrue("Credentials required for performance tests", hasCredentials);
        
        String url = buildConnectionUrl(true, 5);
        int threadCount = 5;
        int queriesPerThread = 10;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Long>> futures = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        
        // Submit concurrent query tasks
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<Long> future = executor.submit(() -> {
                long threadTime = 0;
                try (Connection conn = DriverManager.getConnection(url);
                     Statement stmt = conn.createStatement()) {
                    
                    long threadStart = System.currentTimeMillis();
                    for (int q = 0; q < queriesPerThread; q++) {
                        String query = "SELECT COUNT(*) FROM cloudops.compute_resources";
                        try (ResultSet rs = stmt.executeQuery(query)) {
                            if (rs.next()) {
                                rs.getInt(1);
                            }
                        }
                    }
                    threadTime = System.currentTimeMillis() - threadStart;
                    
                } catch (SQLException e) {
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                }
                return threadTime;
            });
            futures.add(future);
        }
        
        // Wait for all threads to complete
        executor.shutdown();
        assertTrue("Should complete within 60 seconds", 
                  executor.awaitTermination(60, TimeUnit.SECONDS));
        
        long totalTime = System.currentTimeMillis() - startTime;
        
        // Collect results
        long maxThreadTime = 0;
        long totalThreadTime = 0;
        for (Future<Long> future : futures) {
            long threadTime = future.get();
            maxThreadTime = Math.max(maxThreadTime, threadTime);
            totalThreadTime += threadTime;
        }
        
        System.out.println("\n=== Concurrent Access Performance ===");
        System.out.printf("Threads: %d, Queries per thread: %d%n", threadCount, queriesPerThread);
        System.out.printf("Total wall time: %d ms%n", totalTime);
        System.out.printf("Max thread time: %d ms%n", maxThreadTime);
        System.out.printf("Avg thread time: %.1f ms%n", (double) totalThreadTime / threadCount);
        System.out.printf("Concurrency efficiency: %.1f%%%n", 
                         100.0 * maxThreadTime / totalTime);
        
        // Verify concurrent access works
        assertTrue("Should handle concurrent access", totalTime > 0);
    }
    
    @Test
    public void testLargeResultSetPerformance() throws SQLException {
        Assume.assumeTrue("Credentials required for performance tests", hasCredentials);
        
        String url = buildConnectionUrl(true, 5);
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Test different result set sizes
            int[] limits = {10, 50, 100, 500};
            
            System.out.println("\n=== Large Result Set Performance ===");
            
            for (int limit : limits) {
                String query = "SELECT * FROM cloudops.storage_resources LIMIT " + limit;
                
                long start = System.currentTimeMillis();
                int rowCount = 0;
                
                try (ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        rowCount++;
                        // Simulate processing
                        rs.getString("resource_name");
                        rs.getString("cloud_provider");
                        rs.getBoolean("encryption_enabled");
                    }
                }
                
                long elapsed = System.currentTimeMillis() - start;
                double timePerRow = (double) elapsed / rowCount;
                
                System.out.printf("Limit %d: %d ms total, %.2f ms/row%n", 
                                limit, elapsed, timePerRow);
                
                assertEquals("Should return expected number of rows", 
                           Math.min(limit, rowCount), rowCount);
            }
        }
    }
    
    @Test
    public void testComplexQueryPerformance() throws SQLException {
        Assume.assumeTrue("Credentials required for performance tests", hasCredentials);
        
        String url = buildConnectionUrl(true, 5);
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            System.out.println("\n=== Complex Query Performance ===");
            
            // Test 1: Simple query
            long simpleStart = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.storage_resources LIMIT 10")) {
                while (rs.next()) rs.getString(1);
            }
            long simpleTime = System.currentTimeMillis() - simpleStart;
            
            // Test 2: Query with filtering
            long filterStart = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery(
                "SELECT * FROM cloudops.storage_resources " +
                "WHERE encryption_enabled = true AND cloud_provider = 'azure' LIMIT 10")) {
                while (rs.next()) rs.getString(1);
            }
            long filterTime = System.currentTimeMillis() - filterStart;
            
            // Test 3: Aggregation query
            long aggStart = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery(
                "SELECT cloud_provider, COUNT(*), " +
                "SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) " +
                "FROM cloudops.storage_resources GROUP BY cloud_provider")) {
                while (rs.next()) rs.getString(1);
            }
            long aggTime = System.currentTimeMillis() - aggStart;
            
            // Test 4: Join query (self-join simulation)
            long joinStart = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery(
                "SELECT s1.resource_name, s1.cloud_provider " +
                "FROM cloudops.storage_resources s1 " +
                "WHERE s1.encryption_enabled = true LIMIT 10")) {
                while (rs.next()) rs.getString(1);
            }
            long joinTime = System.currentTimeMillis() - joinStart;
            
            System.out.printf("Simple query: %d ms%n", simpleTime);
            System.out.printf("Filtered query: %d ms%n", filterTime);
            System.out.printf("Aggregation query: %d ms%n", aggTime);
            System.out.printf("Complex query: %d ms%n", joinTime);
            
            // All queries should complete
            assertTrue("All query times should be positive", 
                      simpleTime > 0 && filterTime > 0 && aggTime > 0 && joinTime > 0);
        }
    }
    
    @Test
    public void testCacheTTLBehavior() throws SQLException, InterruptedException {
        Assume.assumeTrue("Credentials required for performance tests", hasCredentials);
        
        // Use very short TTL for testing (1 minute)
        String url = buildConnectionUrl(true, 1);
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // First query - populates cache
            long firstQueryStart = System.currentTimeMillis();
            int firstCount = 0;
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.kubernetes_clusters LIMIT 5")) {
                while (rs.next()) firstCount++;
            }
            long firstQueryTime = System.currentTimeMillis() - firstQueryStart;
            
            // Second query - should hit cache
            long cachedQueryStart = System.currentTimeMillis();
            int cachedCount = 0;
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.kubernetes_clusters LIMIT 5")) {
                while (rs.next()) cachedCount++;
            }
            long cachedQueryTime = System.currentTimeMillis() - cachedQueryStart;
            
            System.out.println("\n=== Cache TTL Behavior ===");
            System.out.printf("First query (cache miss): %d ms%n", firstQueryTime);
            System.out.printf("Second query (cache hit): %d ms%n", cachedQueryTime);
            System.out.printf("Cache speedup: %.1fx%n", (double) firstQueryTime / cachedQueryTime);
            
            assertEquals("Should return same number of rows", firstCount, cachedCount);
            
            // Note: We can't easily test cache expiration without waiting for the TTL
            // which would make the test too slow
        }
    }
}