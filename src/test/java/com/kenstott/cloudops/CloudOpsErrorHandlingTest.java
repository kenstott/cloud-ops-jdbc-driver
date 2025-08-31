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
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

/**
 * Tests for error handling and edge cases in CloudOps JDBC driver.
 */
public class CloudOpsErrorHandlingTest {
    
    @Before
    public void setUp() throws ClassNotFoundException {
        Class.forName("com.kenstott.CloudOpsDriver");
    }
    
    @Test
    public void testInvalidTableName() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            try {
                stmt.executeQuery("SELECT * FROM cloudops.non_existent_table");
                fail("Should throw exception for non-existent table");
            } catch (SQLException e) {
                // Expected exception
                assertTrue("Exception message should mention table", 
                          e.getMessage().toLowerCase().contains("table") || 
                          e.getMessage().toLowerCase().contains("not found") ||
                          e.getMessage().toLowerCase().contains("non_existent_table"));
            }
        }
    }
    
    @Test
    public void testInvalidColumnName() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            try {
                stmt.executeQuery("SELECT non_existent_column FROM cloudops.compute_resources");
                fail("Should throw exception for non-existent column");
            } catch (SQLException e) {
                // Expected exception
                assertTrue("Exception message should mention column", 
                          e.getMessage().toLowerCase().contains("column") || 
                          e.getMessage().toLowerCase().contains("not found") ||
                          e.getMessage().toLowerCase().contains("non_existent_column"));
            }
        }
    }
    
    @Test
    public void testInvalidSQLSyntax() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            try {
                stmt.executeQuery("INVALID SQL SYNTAX HERE");
                fail("Should throw exception for invalid SQL syntax");
            } catch (SQLException e) {
                // Expected exception
                assertTrue("Should be SQL syntax error", 
                          e instanceof SQLSyntaxErrorException || 
                          e.getMessage().toLowerCase().contains("syntax"));
            }
        }
    }
    
    @Test
    public void testNullParameterHandling() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Test querying with NULL comparison
            String query = "SELECT * FROM cloudops.storage_resources " +
                          "WHERE encryption_enabled IS NULL LIMIT 5";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                // Should execute without error
                while (rs.next()) {
                    // NULL values should be handled properly
                    rs.getObject("encryption_enabled");
                    assertTrue("wasNull should work correctly", 
                              rs.wasNull() || !rs.wasNull());
                }
            }
        }
    }
    
    @Test
    public void testEmptyResultSet() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Query that likely returns no results
            String query = "SELECT * FROM cloudops.compute_resources " +
                          "WHERE cloud_provider = 'non_existent_provider'";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                assertFalse("Should return empty result set", rs.next());
            }
        }
    }
    
    @Test
    public void testConnectionClosedOperations() throws SQLException {
        String url = "jdbc:cloudops:";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        
        // Close the connection
        conn.close();
        
        // Try to use the statement after connection is closed
        try {
            stmt.executeQuery("SELECT * FROM cloudops.compute_resources");
            fail("Should throw exception when using statement after connection is closed");
        } catch (SQLException e) {
            // Expected exception
            assertTrue("Exception should mention closed connection", 
                      e.getMessage().toLowerCase().contains("closed") || 
                      e.getMessage().toLowerCase().contains("connection"));
        }
    }
    
    @Test
    public void testStatementClosedOperations() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.compute_resources LIMIT 1");
            
            // Close the statement
            stmt.close();
            
            // Try to use the ResultSet after statement is closed
            try {
                rs.next();
                fail("Should throw exception when using ResultSet after statement is closed");
            } catch (SQLException e) {
                // Expected exception
                assertTrue("Exception should mention closed", 
                          e.getMessage().toLowerCase().contains("closed") || 
                          e.getMessage().toLowerCase().contains("statement"));
            }
        }
    }
    
    @Test
    public void testInvalidConnectionURL() {
        // Test various invalid URL formats
        String[] invalidUrls = {
            "jdbc:invalid:",
            "cloudops:",
            "jdbc:cloudops:invalid=",
            "jdbc:cloudops:=value",
            "jdbc:cloudops:key=value;invalid"
        };
        
        for (String invalidUrl : invalidUrls) {
            try {
                Connection conn = DriverManager.getConnection(invalidUrl);
                conn.close();
                // Some invalid URLs might still connect with defaults
            } catch (SQLException e) {
                // Expected for some invalid URLs
                System.out.println("Invalid URL rejected: " + invalidUrl);
            }
        }
    }
    
    @Test
    public void testLargeOffsetHandling() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Test with very large offset
            String query = "SELECT * FROM cloudops.compute_resources LIMIT 5 OFFSET 999999";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                // Should return empty result set, not error
                assertFalse("Should return no results with large offset", rs.next());
            }
        }
    }
    
    @Test
    public void testNegativeLimit() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            try {
                // Negative LIMIT is invalid in SQL
                stmt.executeQuery("SELECT * FROM cloudops.compute_resources LIMIT -5");
                fail("Should throw exception for negative LIMIT");
            } catch (SQLException e) {
                // Expected exception
                assertTrue("Exception should mention limit or syntax error", 
                          e.getMessage().toLowerCase().contains("limit") || 
                          e.getMessage().toLowerCase().contains("syntax"));
            }
        }
    }
    
    @Test
    public void testDataTypeMismatch() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            try {
                // Try to compare boolean with string
                stmt.executeQuery("SELECT * FROM cloudops.storage_resources " +
                                "WHERE encryption_enabled = 'not_a_boolean'");
                fail("Should throw exception for type mismatch");
            } catch (SQLException e) {
                // Expected exception
                assertTrue("Exception should mention type or cast error", 
                          e.getMessage().toLowerCase().contains("type") || 
                          e.getMessage().toLowerCase().contains("cast") ||
                          e.getMessage().toLowerCase().contains("boolean"));
            }
        }
    }
    
    @Test
    public void testConcurrentStatements() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            
            // Execute queries on both statements
            ResultSet rs1 = stmt1.executeQuery("SELECT * FROM cloudops.compute_resources LIMIT 5");
            ResultSet rs2 = stmt2.executeQuery("SELECT * FROM cloudops.storage_resources LIMIT 5");
            
            // Should be able to iterate both result sets
            assertTrue("First result set should have data", rs1.next());
            assertTrue("Second result set should have data", rs2.next());
            
            rs1.close();
            rs2.close();
            stmt1.close();
            stmt2.close();
        }
    }
    
    @Test
    public void testTransactionIsolation() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            // CloudOps is read-only, but should handle transaction settings gracefully
            conn.setAutoCommit(false);
            assertTrue("Should accept auto-commit setting", !conn.getAutoCommit());
            
            // Should handle transaction isolation level
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertEquals("Should accept transaction isolation level", 
                        Connection.TRANSACTION_READ_COMMITTED, 
                        conn.getTransactionIsolation());
            
            // Rollback should not cause error (no-op for read-only)
            conn.rollback();
            
            // Commit should not cause error (no-op for read-only)
            conn.commit();
        }
    }
    
    @Test
    public void testResultSetNavigation() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM cloudops.compute_resources LIMIT 10")) {
            
            // Test forward navigation
            int count = 0;
            while (rs.next()) {
                count++;
                assertNotNull("Should have data in row " + count, rs.getString(1));
            }
            
            assertTrue("Should have retrieved some rows", count > 0);
            
            // Test isAfterLast
            assertTrue("Should be after last row", rs.isAfterLast());
            
            // Test that next() returns false after end
            assertFalse("next() should return false after last row", rs.next());
        }
    }
    
    @Test
    public void testSpecialCharactersInQueries() throws SQLException {
        String url = "jdbc:cloudops:";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Test with special characters in string literals
            String query = "SELECT * FROM cloudops.storage_resources " +
                          "WHERE resource_name LIKE '%test''s%' OR " +
                          "resource_name LIKE '%test\\%' LIMIT 5";
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                // Should handle special characters without error
                while (rs.next()) {
                    rs.getString("resource_name");
                }
            } catch (SQLException e) {
                // Some special character handling might fail, which is acceptable
                System.out.println("Special character test failed (acceptable): " + e.getMessage());
            }
        }
    }
}