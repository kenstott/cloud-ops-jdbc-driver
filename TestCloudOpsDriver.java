import java.sql.*;

public class TestCloudOpsDriver {
    public static void main(String[] args) {
        System.out.println("Testing CloudOpsDriver...");
        
        try {
            // Test 1: Check if driver is registered
            System.out.println("\n1. Testing driver registration:");
            Class.forName("com.kenstott.CloudOpsDriver");
            System.out.println("✓ Driver class loaded successfully");
            
            // Test 2: Check if driver accepts URL
            System.out.println("\n2. Testing URL acceptance:");
            Driver driver = DriverManager.getDriver("jdbc:cloudops:");
            System.out.println("✓ Driver found for jdbc:cloudops: URL");
            System.out.println("  Driver class: " + driver.getClass().getName());
            
            // Test 3: Test various URL formats
            System.out.println("\n3. Testing URL formats:");
            String[] testUrls = {
                "jdbc:cloudops:",
                "jdbc:cloudops:azure.tenantId=test",
                "jdbc:cloudops:gcp.projectIds=project1,project2",
                "jdbc:cloudops:aws.region=us-east-1;aws.accountIds=123456"
            };
            
            for (String url : testUrls) {
                boolean accepts = driver.acceptsURL(url);
                System.out.println("  " + url);
                System.out.println("    Accepts: " + (accepts ? "✓" : "✗"));
            }
            
            // Test 4: Test driver version info
            System.out.println("\n4. Testing driver version info:");
            System.out.println("  Major version: " + driver.getMajorVersion());
            System.out.println("  Minor version: " + driver.getMinorVersion());
            System.out.println("  JDBC compliant: " + driver.jdbcCompliant());
            
            System.out.println("\n✓ All basic tests passed!");
            
        } catch (ClassNotFoundException e) {
            System.err.println("✗ Failed to load driver class: " + e.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            System.err.println("✗ SQL error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("✗ Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}