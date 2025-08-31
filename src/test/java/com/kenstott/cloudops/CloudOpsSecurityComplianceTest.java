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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Tests for security compliance and governance queries.
 * These tests verify the ability to query security-related cloud resource configurations.
 */
public class CloudOpsSecurityComplianceTest {
    
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
    public void testKubernetesSecurityPosture() throws SQLException {
        Assume.assumeTrue("Credentials required for security tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query for Kubernetes clusters with security issues
            String query = "SELECT cluster_name, cloud_provider, " +
                          "rbac_enabled, private_cluster, public_endpoint, " +
                          "encryption_at_rest_enabled, network_policy_provider " +
                          "FROM cloudops.kubernetes_clusters " +
                          "WHERE rbac_enabled = false OR public_endpoint = true OR encryption_at_rest_enabled = false";
            
            List<Map<String, Object>> securityIssues = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    Map<String, Object> issue = new HashMap<>();
                    issue.put("cluster", rs.getString("cluster_name"));
                    issue.put("provider", rs.getString("cloud_provider"));
                    issue.put("rbac_enabled", rs.getBoolean("rbac_enabled"));
                    issue.put("private_cluster", rs.getBoolean("private_cluster"));
                    issue.put("public_endpoint", rs.getBoolean("public_endpoint"));
                    issue.put("encryption_enabled", rs.getBoolean("encryption_at_rest_enabled"));
                    securityIssues.add(issue);
                }
            }
            
            // Log security findings
            System.out.println("Found " + securityIssues.size() + " clusters with potential security concerns");
            for (Map<String, Object> issue : securityIssues) {
                System.out.println("  - " + issue.get("cluster") + " (" + issue.get("provider") + "):");
                if (!(Boolean)issue.get("rbac_enabled")) {
                    System.out.println("    * RBAC not enabled");
                }
                if ((Boolean)issue.get("public_endpoint")) {
                    System.out.println("    * Public endpoint exposed");
                }
                if (!(Boolean)issue.get("encryption_enabled")) {
                    System.out.println("    * Encryption at rest not enabled");
                }
            }
        }
    }
    
    @Test
    public void testStorageEncryptionCompliance() throws SQLException {
        Assume.assumeTrue("Credentials required for security tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query for unencrypted storage resources
            String query = "SELECT cloud_provider, COUNT(*) as unencrypted_count " +
                          "FROM cloudops.storage_resources " +
                          "WHERE encryption_enabled = false OR encryption_enabled IS NULL " +
                          "GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("Unencrypted storage resources by provider:");
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    int count = rs.getInt("unencrypted_count");
                    System.out.println("  " + provider + ": " + count);
                    
                    // Verify we can identify compliance issues
                    assertTrue("Count should be non-negative", count >= 0);
                }
            }
            
            // Query for storage with customer-managed keys
            String cmkQuery = "SELECT resource_name, cloud_provider, encryption_key_type " +
                             "FROM cloudops.storage_resources " +
                             "WHERE encryption_key_type = 'customer-managed' LIMIT 10";
            
            try (ResultSet rs = stmt.executeQuery(cmkQuery)) {
                System.out.println("\nStorage using customer-managed keys:");
                while (rs.next()) {
                    System.out.println("  - " + rs.getString("resource_name") + 
                                     " (" + rs.getString("cloud_provider") + ")");
                }
            }
        }
    }
    
    @Test
    public void testPublicAccessExposure() throws SQLException {
        Assume.assumeTrue("Credentials required for security tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query for storage resources with public access
            String query = "SELECT resource_name, cloud_provider, public_access_enabled, public_access_level " +
                          "FROM cloudops.storage_resources " +
                          "WHERE public_access_enabled = true LIMIT 20";
            
            int publicResourceCount = 0;
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("Storage resources with public access:");
                while (rs.next()) {
                    publicResourceCount++;
                    System.out.println("  - " + rs.getString("resource_name") + 
                                     " (" + rs.getString("cloud_provider") + ")" +
                                     " Level: " + rs.getString("public_access_level"));
                }
            }
            
            System.out.println("Total public resources found: " + publicResourceCount);
        }
    }
    
    @Test
    public void testNetworkSecurityConfiguration() throws SQLException {
        Assume.assumeTrue("Credentials required for security tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query for Kubernetes clusters without network policies
            String query = "SELECT cluster_name, cloud_provider, network_policy_provider, authorized_ip_ranges " +
                          "FROM cloudops.kubernetes_clusters " +
                          "WHERE network_policy_provider IS NULL OR authorized_ip_ranges = 0";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("Clusters with potential network security gaps:");
                while (rs.next()) {
                    String clusterName = rs.getString("cluster_name");
                    String provider = rs.getString("cloud_provider");
                    String networkPolicy = rs.getString("network_policy_provider");
                    int authorizedRanges = rs.getInt("authorized_ip_ranges");
                    
                    System.out.println("  - " + clusterName + " (" + provider + ")");
                    if (networkPolicy == null) {
                        System.out.println("    * No network policy provider");
                    }
                    if (authorizedRanges == 0) {
                        System.out.println("    * No IP range restrictions");
                    }
                }
            }
        }
    }
    
    @Test
    public void testComplianceSummaryReport() throws SQLException {
        Assume.assumeTrue("Credentials required for security tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Generate compliance summary across all resources
            Map<String, Integer> complianceMetrics = new HashMap<>();
            
            // Check Kubernetes compliance
            String k8sQuery = "SELECT " +
                             "COUNT(*) as total, " +
                             "SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) as rbac_compliant, " +
                             "SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) as private_clusters, " +
                             "SUM(CASE WHEN encryption_at_rest_enabled = true THEN 1 ELSE 0 END) as encrypted_clusters " +
                             "FROM cloudops.kubernetes_clusters";
            
            try (ResultSet rs = stmt.executeQuery(k8sQuery)) {
                if (rs.next()) {
                    complianceMetrics.put("k8s_total", rs.getInt("total"));
                    complianceMetrics.put("k8s_rbac_compliant", rs.getInt("rbac_compliant"));
                    complianceMetrics.put("k8s_private", rs.getInt("private_clusters"));
                    complianceMetrics.put("k8s_encrypted", rs.getInt("encrypted_clusters"));
                }
            }
            
            // Check storage compliance
            String storageQuery = "SELECT " +
                                 "COUNT(*) as total, " +
                                 "SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) as encrypted, " +
                                 "SUM(CASE WHEN public_access_enabled = false OR public_access_enabled IS NULL THEN 1 ELSE 0 END) as private, " +
                                 "SUM(CASE WHEN versioning_enabled = true THEN 1 ELSE 0 END) as versioned " +
                                 "FROM cloudops.storage_resources";
            
            try (ResultSet rs = stmt.executeQuery(storageQuery)) {
                if (rs.next()) {
                    complianceMetrics.put("storage_total", rs.getInt("total"));
                    complianceMetrics.put("storage_encrypted", rs.getInt("encrypted"));
                    complianceMetrics.put("storage_private", rs.getInt("private"));
                    complianceMetrics.put("storage_versioned", rs.getInt("versioned"));
                }
            }
            
            // Print compliance report
            System.out.println("\n=== Security Compliance Report ===");
            System.out.println("\nKubernetes Clusters:");
            if (complianceMetrics.get("k8s_total") != null && complianceMetrics.get("k8s_total") > 0) {
                int total = complianceMetrics.get("k8s_total");
                System.out.println("  Total Clusters: " + total);
                System.out.println("  RBAC Enabled: " + complianceMetrics.get("k8s_rbac_compliant") + 
                                 " (" + (100 * complianceMetrics.get("k8s_rbac_compliant") / total) + "%)");
                System.out.println("  Private Clusters: " + complianceMetrics.get("k8s_private") + 
                                 " (" + (100 * complianceMetrics.get("k8s_private") / total) + "%)");
                System.out.println("  Encryption Enabled: " + complianceMetrics.get("k8s_encrypted") + 
                                 " (" + (100 * complianceMetrics.get("k8s_encrypted") / total) + "%)");
            }
            
            System.out.println("\nStorage Resources:");
            if (complianceMetrics.get("storage_total") != null && complianceMetrics.get("storage_total") > 0) {
                int total = complianceMetrics.get("storage_total");
                System.out.println("  Total Resources: " + total);
                System.out.println("  Encrypted: " + complianceMetrics.get("storage_encrypted") + 
                                 " (" + (100 * complianceMetrics.get("storage_encrypted") / total) + "%)");
                System.out.println("  Private Access: " + complianceMetrics.get("storage_private") + 
                                 " (" + (100 * complianceMetrics.get("storage_private") / total) + "%)");
                System.out.println("  Versioning Enabled: " + complianceMetrics.get("storage_versioned") + 
                                 " (" + (100 * complianceMetrics.get("storage_versioned") / total) + "%)");
            }
            
            // Assertions to verify we can generate compliance metrics
            assertTrue("Should have compliance metrics", complianceMetrics.size() > 0);
        }
    }
    
    @Test
    public void testCrossCloudSecurityComparison() throws SQLException {
        Assume.assumeTrue("Credentials required for security tests", hasCredentials);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Compare security posture across cloud providers
            String query = "SELECT cloud_provider, " +
                          "AVG(CASE WHEN rbac_enabled = true THEN 1.0 ELSE 0.0 END) as rbac_score, " +
                          "AVG(CASE WHEN private_cluster = true THEN 1.0 ELSE 0.0 END) as private_score, " +
                          "AVG(CASE WHEN encryption_at_rest_enabled = true THEN 1.0 ELSE 0.0 END) as encryption_score " +
                          "FROM cloudops.kubernetes_clusters " +
                          "GROUP BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\nSecurity scores by cloud provider (0-1 scale):");
                while (rs.next()) {
                    String provider = rs.getString("cloud_provider");
                    double rbacScore = rs.getDouble("rbac_score");
                    double privateScore = rs.getDouble("private_score");
                    double encryptionScore = rs.getDouble("encryption_score");
                    double overallScore = (rbacScore + privateScore + encryptionScore) / 3.0;
                    
                    System.out.println("  " + provider + ":");
                    System.out.printf("    RBAC: %.2f, Private: %.2f, Encryption: %.2f, Overall: %.2f%n",
                                    rbacScore, privateScore, encryptionScore, overallScore);
                }
            }
        }
    }
}