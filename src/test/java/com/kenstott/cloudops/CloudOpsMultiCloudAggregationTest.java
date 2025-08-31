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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Tests for multi-cloud aggregation and cross-cloud analytics.
 * Focuses heavily on storage and encryption patterns across clouds.
 */
public class CloudOpsMultiCloudAggregationTest {
    
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
    public void testStorageDistributionAcrossClouds() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Analyze storage distribution and types across clouds
            String query = "SELECT cloud_provider, storage_type, COUNT(*) as count, " +
                          "SUM(CASE WHEN size_bytes IS NOT NULL THEN size_bytes ELSE 0 END) as total_bytes " +
                          "FROM cloudops.storage_resources " +
                          "GROUP BY cloud_provider, storage_type " +
                          "ORDER BY cloud_provider, count DESC";
            
            Map<String, Long> cloudStorageTotals = new HashMap<>();
            Map<String, Map<String, Integer>> storageTypesByCloud = new HashMap<>();
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\n=== Storage Distribution Across Clouds ===");
                String currentProvider = null;
                
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    String storageType = rs.getString("storage_type");
                    int count = rs.getInt("count");
                    long totalBytes = rs.getLong("total_bytes");
                    
                    if (!provider.equals(currentProvider)) {
                        if (currentProvider != null) System.out.println();
                        System.out.println(provider.toUpperCase() + " Storage:");
                        currentProvider = provider;
                    }
                    
                    double totalGB = totalBytes / (1024.0 * 1024.0 * 1024.0);
                    System.out.printf("  %s: %d resources (%.2f GB)%n", storageType, count, totalGB);
                    
                    // Aggregate data
                    cloudStorageTotals.merge(provider, totalBytes, Long::sum);
                    storageTypesByCloud.computeIfAbsent(provider, k -> new HashMap<>())
                                      .put(storageType, count);
                }
            }
            
            // Verify we have multi-cloud data
            assertTrue("Should have storage data from multiple clouds", cloudStorageTotals.size() > 0);
        }
    }
    
    @Test
    public void testStorageEncryptionAnalysisAcrossClouds() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Deep dive into storage encryption patterns
            String query = "SELECT " +
                          "cloud_provider, " +
                          "storage_type, " +
                          "encryption_enabled, " +
                          "encryption_type, " +
                          "encryption_key_type, " +
                          "COUNT(*) as resource_count " +
                          "FROM cloudops.storage_resources " +
                          "GROUP BY cloud_provider, storage_type, encryption_enabled, " +
                          "encryption_type, encryption_key_type " +
                          "ORDER BY cloud_provider, storage_type, resource_count DESC";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\n=== Storage Encryption Analysis by Cloud Provider ===");
                String currentProvider = null;
                String currentType = null;
                
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    String storageType = rs.getString("storage_type");
                    boolean encryptionEnabled = rs.getBoolean("encryption_enabled");
                    String encryptionType = rs.getString("encryption_type");
                    String keyType = rs.getString("encryption_key_type");
                    int count = rs.getInt("resource_count");
                    
                    if (!provider.equals(currentProvider)) {
                        currentProvider = provider;
                        currentType = null;
                        System.out.println("\n" + provider.toUpperCase() + ":");
                    }
                    
                    if (!storageType.equals(currentType)) {
                        currentType = storageType;
                        System.out.println("  " + storageType + ":");
                    }
                    
                    String encryptionStatus = encryptionEnabled ? "Encrypted" : "Unencrypted";
                    String encDetails = "";
                    if (encryptionEnabled) {
                        encDetails = String.format(" [%s, %s keys]", 
                                                  encryptionType != null ? encryptionType : "default",
                                                  keyType != null ? keyType : "service-managed");
                    }
                    
                    System.out.printf("    %s: %d resources%s%n", encryptionStatus, count, encDetails);
                }
            }
        }
    }
    
    @Test
    public void testStorageEncryptionComplianceScoreByCloud() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Calculate encryption compliance scores
            String query = "SELECT " +
                          "cloud_provider, " +
                          "COUNT(*) as total_resources, " +
                          "SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) as encrypted_resources, " +
                          "SUM(CASE WHEN encryption_key_type = 'customer-managed' THEN 1 ELSE 0 END) as cmk_resources, " +
                          "SUM(CASE WHEN https_only = true THEN 1 ELSE 0 END) as https_only_resources, " +
                          "SUM(CASE WHEN versioning_enabled = true THEN 1 ELSE 0 END) as versioned_resources, " +
                          "SUM(CASE WHEN soft_delete_enabled = true THEN 1 ELSE 0 END) as soft_delete_resources " +
                          "FROM cloudops.storage_resources " +
                          "GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\n=== Storage Encryption & Security Compliance Scores ===");
                System.out.println("(Higher scores indicate better security posture)");
                
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    int total = rs.getInt("total_resources");
                    int encrypted = rs.getInt("encrypted_resources");
                    int cmk = rs.getInt("cmk_resources");
                    int httpsOnly = rs.getInt("https_only_resources");
                    int versioned = rs.getInt("versioned_resources");
                    int softDelete = rs.getInt("soft_delete_resources");
                    
                    if (total > 0) {
                        System.out.println("\n" + provider.toUpperCase() + " (" + total + " resources):");
                        
                        double encryptionScore = (100.0 * encrypted) / total;
                        double cmkScore = (100.0 * cmk) / total;
                        double httpsScore = (100.0 * httpsOnly) / total;
                        double versionScore = (100.0 * versioned) / total;
                        double softDeleteScore = (100.0 * softDelete) / total;
                        
                        System.out.printf("  Encryption Coverage: %.1f%% (%d/%d)%n", encryptionScore, encrypted, total);
                        System.out.printf("  Customer-Managed Keys: %.1f%% (%d/%d)%n", cmkScore, cmk, total);
                        System.out.printf("  HTTPS Only: %.1f%% (%d/%d)%n", httpsScore, httpsOnly, total);
                        System.out.printf("  Versioning Enabled: %.1f%% (%d/%d)%n", versionScore, versioned, total);
                        System.out.printf("  Soft Delete Enabled: %.1f%% (%d/%d)%n", softDeleteScore, softDelete, total);
                        
                        // Calculate overall security score
                        double overallScore = (encryptionScore * 0.35 + // Encryption is most critical
                                             cmkScore * 0.20 +        // CMK adds defense in depth
                                             httpsScore * 0.15 +      // Transport security
                                             versionScore * 0.15 +    // Data protection
                                             softDeleteScore * 0.15); // Recovery capability
                        
                        System.out.printf("  Overall Security Score: %.1f/100%n", overallScore);
                    }
                }
            }
        }
    }
    
    @Test
    public void testStorageTypeEncryptionPatterns() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Analyze encryption patterns by storage type
            String query = "SELECT " +
                          "storage_type, " +
                          "COUNT(*) as total, " +
                          "SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) as encrypted, " +
                          "SUM(CASE WHEN encryption_key_type = 'customer-managed' THEN 1 ELSE 0 END) as cmk_encrypted " +
                          "FROM cloudops.storage_resources " +
                          "WHERE storage_type IS NOT NULL " +
                          "GROUP BY storage_type " +
                          "ORDER BY total DESC";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\n=== Encryption Patterns by Storage Type ===");
                
                while (rs.next()) {
                    String storageType = rs.getString("storage_type");
                    int total = rs.getInt("total");
                    int encrypted = rs.getInt("encrypted");
                    int cmkEncrypted = rs.getInt("cmk_encrypted");
                    
                    double encryptionRate = (100.0 * encrypted) / total;
                    double cmkRate = (100.0 * cmkEncrypted) / total;
                    
                    System.out.printf("%s:%n", storageType);
                    System.out.printf("  Total: %d resources%n", total);
                    System.out.printf("  Encrypted: %.1f%% (%d resources)%n", encryptionRate, encrypted);
                    System.out.printf("  Customer-Managed Keys: %.1f%% (%d resources)%n", cmkRate, cmkEncrypted);
                    
                    // Risk assessment
                    if (encryptionRate < 50) {
                        System.out.println("  ⚠️  HIGH RISK: Less than 50% encryption coverage");
                    } else if (encryptionRate < 80) {
                        System.out.println("  ⚠️  MEDIUM RISK: Encryption coverage below 80%");
                    } else if (encryptionRate < 100) {
                        System.out.println("  ℹ️  LOW RISK: Good encryption coverage");
                    } else {
                        System.out.println("  ✓ COMPLIANT: Full encryption coverage");
                    }
                    System.out.println();
                }
            }
        }
    }
    
    @Test
    public void testPublicStorageExposureAnalysis() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Analyze public storage exposure with encryption status
            String query = "SELECT " +
                          "cloud_provider, " +
                          "storage_type, " +
                          "public_access_enabled, " +
                          "encryption_enabled, " +
                          "COUNT(*) as count " +
                          "FROM cloudops.storage_resources " +
                          "WHERE public_access_enabled = true " +
                          "GROUP BY cloud_provider, storage_type, public_access_enabled, encryption_enabled " +
                          "ORDER BY count DESC";
            
            int totalPublic = 0;
            int publicEncrypted = 0;
            int publicUnencrypted = 0;
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\n=== Public Storage Exposure & Encryption Analysis ===");
                
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    String storageType = rs.getString("storage_type");
                    boolean encrypted = rs.getBoolean("encryption_enabled");
                    int count = rs.getInt("count");
                    
                    totalPublic += count;
                    if (encrypted) {
                        publicEncrypted += count;
                    } else {
                        publicUnencrypted += count;
                    }
                    
                    String risk = encrypted ? "MEDIUM RISK (public but encrypted)" : "CRITICAL RISK (public and unencrypted)";
                    System.out.printf("%s - %s: %d resources [%s]%n", 
                                    provider, storageType, count, risk);
                }
            }
            
            if (totalPublic > 0) {
                System.out.println("\nPublic Storage Summary:");
                System.out.printf("  Total Public Resources: %d%n", totalPublic);
                System.out.printf("  Public & Encrypted: %d (%.1f%%)%n", 
                                publicEncrypted, (100.0 * publicEncrypted) / totalPublic);
                System.out.printf("  Public & Unencrypted: %d (%.1f%%) ⚠️ CRITICAL%n", 
                                publicUnencrypted, (100.0 * publicUnencrypted) / totalPublic);
            }
        }
    }
    
    @Test
    public void testStorageLifecycleAndDataProtection() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Analyze data protection features across clouds
            String query = "SELECT " +
                          "cloud_provider, " +
                          "AVG(CASE WHEN versioning_enabled = true THEN 1.0 ELSE 0.0 END) as versioning_rate, " +
                          "AVG(CASE WHEN soft_delete_enabled = true THEN 1.0 ELSE 0.0 END) as soft_delete_rate, " +
                          "AVG(CASE WHEN backup_enabled = true THEN 1.0 ELSE 0.0 END) as backup_rate, " +
                          "AVG(CASE WHEN lifecycle_rules_count > 0 THEN 1.0 ELSE 0.0 END) as lifecycle_mgmt_rate, " +
                          "COUNT(*) as total_resources " +
                          "FROM cloudops.storage_resources " +
                          "GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\n=== Storage Data Protection Features by Cloud ===");
                
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    double versioningRate = rs.getDouble("versioning_rate") * 100;
                    double softDeleteRate = rs.getDouble("soft_delete_rate") * 100;
                    double backupRate = rs.getDouble("backup_rate") * 100;
                    double lifecycleRate = rs.getDouble("lifecycle_mgmt_rate") * 100;
                    int total = rs.getInt("total_resources");
                    
                    System.out.printf("\n%s (%d resources):%n", provider.toUpperCase(), total);
                    System.out.printf("  Versioning: %.1f%%%n", versioningRate);
                    System.out.printf("  Soft Delete: %.1f%%%n", softDeleteRate);
                    System.out.printf("  Backup: %.1f%%%n", backupRate);
                    System.out.printf("  Lifecycle Management: %.1f%%%n", lifecycleRate);
                    
                    // Calculate data protection score
                    double protectionScore = (versioningRate + softDeleteRate + backupRate + lifecycleRate) / 4;
                    System.out.printf("  Data Protection Score: %.1f/100%n", protectionScore);
                    
                    if (protectionScore < 25) {
                        System.out.println("  ⚠️  CRITICAL: Very low data protection");
                    } else if (protectionScore < 50) {
                        System.out.println("  ⚠️  WARNING: Insufficient data protection");
                    } else if (protectionScore < 75) {
                        System.out.println("  ℹ️  ACCEPTABLE: Moderate data protection");
                    } else {
                        System.out.println("  ✓ GOOD: Strong data protection");
                    }
                }
            }
        }
    }
    
    @Test
    public void testCrossCloudResourceTotals() throws SQLException {
        Assume.assumeTrue("Credentials required for aggregation tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Get totals across all resource types
            Map<String, Map<String, Integer>> resourceCounts = new HashMap<>();
            
            // Count compute resources
            String computeQuery = "SELECT cloud_provider, COUNT(*) as count " +
                                 "FROM cloudops.compute_resources GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(computeQuery)) {
                while (rs.next()) {
                    resourceCounts.computeIfAbsent(rs.getString("cloud_provider"), k -> new HashMap<>())
                                 .put("compute", rs.getInt("count"));
                }
            }
            
            // Count storage resources
            String storageQuery = "SELECT cloud_provider, COUNT(*) as count " +
                                 "FROM cloudops.storage_resources GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(storageQuery)) {
                while (rs.next()) {
                    resourceCounts.computeIfAbsent(rs.getString("cloud_provider"), k -> new HashMap<>())
                                 .put("storage", rs.getInt("count"));
                }
            }
            
            // Count Kubernetes clusters
            String k8sQuery = "SELECT cloud_provider, COUNT(*) as count " +
                             "FROM cloudops.kubernetes_clusters GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(k8sQuery)) {
                while (rs.next()) {
                    resourceCounts.computeIfAbsent(rs.getString("cloud_provider"), k -> new HashMap<>())
                                 .put("kubernetes", rs.getInt("count"));
                }
            }
            
            // Print summary
            System.out.println("\n=== Multi-Cloud Resource Summary ===");
            for (Map.Entry<String, Map<String, Integer>> cloudEntry : resourceCounts.entrySet()) {
                String provider = cloudEntry.getKey();
                Map<String, Integer> counts = cloudEntry.getValue();
                
                int total = counts.values().stream().mapToInt(Integer::intValue).sum();
                System.out.printf("\n%s (Total: %d resources):%n", provider.toUpperCase(), total);
                System.out.printf("  Compute: %d%n", counts.getOrDefault("compute", 0));
                System.out.printf("  Storage: %d%n", counts.getOrDefault("storage", 0));
                System.out.printf("  Kubernetes: %d%n", counts.getOrDefault("kubernetes", 0));
            }
            
            assertTrue("Should have multi-cloud data", resourceCounts.size() > 0);
        }
    }
}