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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Integration tests for CloudOpsDriver.
 * 
 * These tests require actual cloud credentials configured in local-test.properties.
 * They will be skipped if the properties file doesn't exist.
 */
public class CloudOpsIntegrationTest {
    
    private Properties testProperties;
    private boolean azureConfigured = false;
    private boolean gcpConfigured = false;
    private boolean awsConfigured = false;
    
    @Before
    public void setUp() throws IOException, ClassNotFoundException {
        // Ensure the driver is loaded
        Class.forName("com.kenstott.CloudOpsDriver");
        
        testProperties = new Properties();
        
        // Look for local-test.properties in multiple locations
        File[] propertyFiles = {
            new File("src/test/resources/local-test.properties"),
            new File("local-test.properties"),
            new File(System.getProperty("user.home"), "local-test.properties"),
            new File(System.getProperty("user.home"), ".cloud-governance/local-test.properties")
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
            
            // Check which cloud providers are configured
            azureConfigured = isProviderConfigured("azure.tenantId", "azure.clientId");
            gcpConfigured = isProviderConfigured("gcp.projectIds", "gcp.credentialsPath");
            awsConfigured = isProviderConfigured("aws.accountIds", "aws.accessKeyId");
        } else {
            System.out.println("No local-test.properties found. Integration tests will be skipped.");
            System.out.println("To run integration tests, create local-test.properties from the sample file.");
        }
    }
    
    private boolean isProviderConfigured(String... requiredKeys) {
        for (String key : requiredKeys) {
            String value = testProperties.getProperty(key);
            if (value == null || value.trim().isEmpty() || 
                value.contains("your-") || value.contains("/path/to/")) {
                return false;
            }
        }
        return true;
    }
    
    private String buildConnectionUrl() {
        StringBuilder url = new StringBuilder("jdbc:cloudops:");
        boolean first = true;
        
        // Add Azure parameters if configured
        if (azureConfigured) {
            for (String key : testProperties.stringPropertyNames()) {
                if (key.startsWith("azure.")) {
                    if (!first) url.append(";");
                    url.append(key).append("=").append(testProperties.getProperty(key));
                    first = false;
                }
            }
        }
        
        // Add GCP parameters if configured
        if (gcpConfigured) {
            for (String key : testProperties.stringPropertyNames()) {
                if (key.startsWith("gcp.")) {
                    if (!first) url.append(";");
                    url.append(key).append("=").append(testProperties.getProperty(key));
                    first = false;
                }
            }
        }
        
        // Add AWS parameters if configured
        if (awsConfigured) {
            for (String key : testProperties.stringPropertyNames()) {
                if (key.startsWith("aws.")) {
                    if (!first) url.append(";");
                    url.append(key).append("=").append(testProperties.getProperty(key));
                    first = false;
                }
            }
        }
        
        return url.toString();
    }
    
    @Test
    public void testConnection() throws SQLException {
        Assume.assumeTrue("No cloud providers configured", 
                         azureConfigured || gcpConfigured || awsConfigured);
        
        String url = buildConnectionUrl();
        System.out.println("Testing connection with URL: " + url.replaceAll("(Secret|Key)=[^;]+", "$1=***"));
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            assertFalse("Connection should not be closed", conn.isClosed());
        }
    }
    
    @Test
    public void testDatabaseMetadata() throws SQLException {
        Assume.assumeTrue("No cloud providers configured", 
                         azureConfigured || gcpConfigured || awsConfigured);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData metadata = conn.getMetaData();
            assertNotNull("Metadata should not be null", metadata);
            
            // Check driver info
            System.out.println("Driver Name: " + metadata.getDriverName());
            System.out.println("Driver Version: " + metadata.getDriverVersion());
            System.out.println("Database Product: " + metadata.getDatabaseProductName());
            System.out.println("Database Version: " + metadata.getDatabaseProductVersion());
        }
    }
    
    @Test
    public void testListTables() throws SQLException {
        Assume.assumeTrue("No cloud providers configured", 
                         azureConfigured || gcpConfigured || awsConfigured);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData metadata = conn.getMetaData();
            
            // List tables
            try (ResultSet tables = metadata.getTables(null, "cloudops", null, new String[]{"TABLE"})) {
                System.out.println("\nAvailable tables:");
                int tableCount = 0;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    tableCount++;
                }
                
                // Should have the standard Cloud Governance tables
                assertTrue("Should have at least one table", tableCount > 0);
            }
        }
    }
    
    @Test
    public void testQueryComputeResources() throws SQLException {
        Assume.assumeTrue("No cloud providers configured", 
                         azureConfigured || gcpConfigured || awsConfigured);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query compute resources with correct column names
            String query = "SELECT resource_id, instance_name, instance_type, cloud_provider " +
                          "FROM compute_resources LIMIT 10";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\nCompute Resources (max 10):");
                int count = 0;
                while (rs.next()) {
                    System.out.printf("  %s | %s | %s | %s%n",
                        rs.getString("resource_id"),
                        rs.getString("instance_name"),
                        rs.getString("instance_type"),
                        rs.getString("cloud_provider"));
                    count++;
                }
                System.out.println("Total rows retrieved: " + count);
            }
        }
    }
    
    @Test
    public void testQueryStorageResources() throws SQLException {
        Assume.assumeTrue("AWS configured", awsConfigured);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query storage resources with correct column names
            String query = "SELECT resource_id, resource_name, storage_type, size_bytes " +
                          "FROM storage_resources " +
                          "WHERE cloud_provider = 'AWS' LIMIT 10";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\nAWS Storage Resources (max 10):");
                int count = 0;
                while (rs.next()) {
                    long sizeBytes = rs.getLong("size_bytes");
                    double sizeGB = sizeBytes / (1024.0 * 1024.0 * 1024.0);
                    System.out.printf("  %s | %s | %s | %.2f GB%n",
                        rs.getString("resource_id"),
                        rs.getString("resource_name"),
                        rs.getString("storage_type"),
                        sizeGB);
                    count++;
                }
                System.out.println("Total rows retrieved: " + count);
            }
        }
    }
    
    @Test
    public void testQueryKubernetesClusters() throws SQLException {
        Assume.assumeTrue("At least one cloud provider configured", 
                         azureConfigured || gcpConfigured || awsConfigured);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query Kubernetes clusters
            String query = "SELECT cluster_name, cloud_provider, kubernetes_version, node_count " +
                          "FROM kubernetes_clusters";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\nKubernetes Clusters:");
                int count = 0;
                while (rs.next()) {
                    System.out.printf("  %s | %s | %s | %d nodes%n",
                        rs.getString("cluster_name"),
                        rs.getString("cloud_provider"),
                        rs.getString("kubernetes_version"),
                        rs.getInt("node_count"));
                    count++;
                }
                System.out.println("Total clusters: " + count);
            }
        }
    }
    
    @Test
    public void testCrossCloudQuery() throws SQLException {
        // Only run if multiple providers are configured
        int configuredProviders = 0;
        if (azureConfigured) configuredProviders++;
        if (gcpConfigured) configuredProviders++;
        if (awsConfigured) configuredProviders++;
        
        Assume.assumeTrue("Multiple cloud providers required", configuredProviders > 1);
        
        String url = buildConnectionUrl();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query resources across all configured clouds
            String query = "SELECT cloud_provider, COUNT(*) as resource_count " +
                          "FROM compute_resources " +
                          "GROUP BY cloud_provider " +
                          "ORDER BY cloud_provider";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("\nResource count by cloud provider:");
                while (rs.next()) {
                    System.out.printf("  %s: %d resources%n",
                        rs.getString("cloud_provider"),
                        rs.getInt("resource_count"));
                }
            }
        }
    }
}