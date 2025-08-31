# Cloud Ops JDBC Driver

A **single pane of glass** for all your cloud operations - query Azure, AWS, and GCP resources using standard SQL.

## Overview

This JDBC driver transforms multi-cloud operations by providing a unified SQL interface to view and analyze all your cloud resources across Azure, GCP, and AWS simultaneously. Instead of jumping between multiple cloud consoles, dashboards, and CLI tools, you get one consistent query interface that aggregates everything.

Perfect for cloud operations, DevOps, SRE teams, and anyone managing multi-cloud infrastructure who needs:
- **Unified visibility** across all cloud providers in one place
- **Cross-cloud analytics** for cost optimization and resource management
- **Operational intelligence** without switching between different tools
- **Standard SQL queries** that work with existing BI and monitoring tools

## Supported Cloud Providers

- **Azure**: Query Azure resources across multiple subscriptions
- **Google Cloud Platform (GCP)**: Access GCP resources across multiple projects  
- **Amazon Web Services (AWS)**: Query AWS resources across multiple accounts

## Connection String Format

```
jdbc:cloudops:paramName=paramValue;paramName2=paramValue2
```

## Examples

### Azure Configuration
```java
String url = "jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1,sub2";
Connection conn = DriverManager.getConnection(url);
```

### GCP Configuration
```java
String url = "jdbc:cloudops:gcp.projectIds=project1,project2;gcp.credentialsPath=/path/to/creds.json";
Connection conn = DriverManager.getConnection(url);
```

### AWS Configuration
```java
String url = "jdbc:cloudops:aws.accountIds=123456;aws.region=us-east-1;aws.accessKeyId=xxx;aws.secretAccessKey=xxx";
Connection conn = DriverManager.getConnection(url);
```

### Multi-Cloud Configuration
```java
String url = "jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;gcp.projectIds=project1;gcp.credentialsPath=/path/to/creds.json";
Connection conn = DriverManager.getConnection(url);
```

### Cache Configuration
```java
// Custom cache settings
String url = "jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;" +
             "cache.enabled=true;cache.ttlMinutes=30;cache.debugMode=false";
Connection conn = DriverManager.getConnection(url);
```

### Using Environment Variables

You can also configure the driver using environment variables instead of query parameters:

```bash
# Azure configuration via environment variables
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_SUBSCRIPTION_IDS="sub1,sub2"

# GCP configuration via environment variables
export GCP_PROJECT_IDS="project1,project2"
export GCP_CREDENTIALS_PATH="/path/to/creds.json"

# AWS configuration via environment variables
export AWS_ACCOUNT_IDS="123456,789012"
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_ROLE_ARN="arn:aws:iam::123456:role/CloudOpsRole"

# Cache configuration via environment variables
export CLOUD_OPS_CACHE_ENABLED="true"
export CLOUD_OPS_CACHE_TTL_MINUTES="15"
export CLOUD_OPS_CACHE_DEBUG_MODE="false"

# Provider configuration
export CLOUD_OPS_PROVIDERS="azure,aws"  # Only enable specific providers
```

```java
// When environment variables are set, you can use a minimal connection string
String url = "jdbc:cloudops:";
Connection conn = DriverManager.getConnection(url);

// Or mix query parameters with environment variables (query parameters take precedence)
String url = "jdbc:cloudops:azure.tenantId=override-tenant";
Connection conn = DriverManager.getConnection(url);
```

## Available Tables

The driver provides access to the following tables:

- `compute_resources` - Virtual machines, containers, and compute instances
- `storage_resources` - Storage accounts, buckets, and file systems
- `network_resources` - Virtual networks, subnets, and network interfaces
- `database_resources` - Database servers and instances
- `kubernetes_clusters` - Kubernetes/AKS/GKE/EKS clusters
- `container_registries` - Container registries across clouds
- `iam_resources` - Identity and access management resources

## Why a Single Pane of Glass?

Traditional multi-cloud management requires:
- Switching between Azure Portal, AWS Console, and GCP Console
- Learning different query languages and APIs for each provider
- Building custom scripts to aggregate data across clouds
- Maintaining separate dashboards for each cloud provider
- Manual correlation of data across different platforms

**With Cloud Ops JDBC Driver, you get:**
- One SQL interface for all clouds - no context switching
- Unified resource view across all providers simultaneously
- Real-time cross-cloud analytics and cost comparisons
- Integration with existing BI tools (Tableau, Power BI, Grafana)
- Consistent data model regardless of underlying cloud provider

## Use Cases

### Cloud Operations & SRE
- **Unified monitoring**: View all resources across clouds in a single query
- **Cost optimization**: Compare costs across providers to identify savings
- **Capacity planning**: Aggregate usage trends across all cloud platforms
- **Incident response**: Query all clouds simultaneously during outages
- **Compliance reporting**: Generate unified compliance reports for multi-cloud

### DevOps & Infrastructure Management
- **Cross-cloud auditing**: Validate configurations across all providers
- **Resource lifecycle**: Track resources regardless of which cloud they're in
- **Security analysis**: Find vulnerabilities across your entire cloud footprint
- **Inventory management**: Maintain a single source of truth for all resources
- **Migration planning**: Compare resources across clouds for migration decisions

### Example Queries - Single Pane of Glass in Action

```sql
-- Single query to see ALL your compute resources across Azure, AWS, and GCP
SELECT cloud_provider, region, instance_name, status, cpu_utilization, monthly_cost 
FROM compute_resources 
ORDER BY monthly_cost DESC;

-- Compare costs across all cloud providers instantly
SELECT 
    cloud_provider,
    COUNT(*) as resource_count,
    SUM(monthly_cost) as total_cost,
    AVG(cpu_utilization) as avg_utilization
FROM compute_resources 
GROUP BY cloud_provider;

-- Find security issues across your entire multi-cloud infrastructure
SELECT cloud_provider, resource_type, resource_name, security_group, open_ports
FROM network_resources 
WHERE open_ports LIKE '%22%' OR open_ports LIKE '%3389%';

-- Identify redundant resources across clouds
SELECT instance_name, COUNT(DISTINCT cloud_provider) as cloud_count
FROM compute_resources 
GROUP BY instance_name 
HAVING COUNT(DISTINCT cloud_provider) > 1;

-- Real-time cost analysis across all clouds
SELECT 
    DATE_TRUNC('month', created_date) as month,
    cloud_provider,
    SUM(monthly_cost) as cost
FROM compute_resources
GROUP BY month, cloud_provider
ORDER BY month DESC;
```

## Parameters

All parameters can be provided either as query parameters in the connection string or as environment variables. Query parameters take precedence over environment variables.

### Azure Parameters
- `azure.tenantId` (env: `AZURE_TENANT_ID`) - Azure tenant ID (required for Azure)
- `azure.clientId` (env: `AZURE_CLIENT_ID`) - Azure client/application ID (required for Azure)
- `azure.clientSecret` (env: `AZURE_CLIENT_SECRET`) - Azure client secret (required for Azure)
- `azure.subscriptionIds` (env: `AZURE_SUBSCRIPTION_IDS`) - Comma-separated list of subscription IDs (required for Azure)

### GCP Parameters
- `gcp.projectIds` (env: `GCP_PROJECT_IDS`) - Comma-separated list of project IDs (required for GCP)
- `gcp.credentialsPath` (env: `GCP_CREDENTIALS_PATH`) - Path to GCP credentials JSON file (required for GCP)

### AWS Parameters
- `aws.accountIds` (env: `AWS_ACCOUNT_IDS`) - Comma-separated list of account IDs (required for AWS)
- `aws.region` (env: `AWS_REGION`) - AWS region (required for AWS)
- `aws.accessKeyId` (env: `AWS_ACCESS_KEY_ID`) - AWS access key ID (required for AWS)
- `aws.secretAccessKey` (env: `AWS_SECRET_ACCESS_KEY`) - AWS secret access key (required for AWS)
- `aws.roleArn` (env: `AWS_ROLE_ARN`) - AWS role ARN for cross-account access (optional)

### Cache Parameters
- `cache.enabled` (env: `CLOUD_OPS_CACHE_ENABLED`) - Enable/disable result caching (default: `true`)
- `cache.ttlMinutes` (env: `CLOUD_OPS_CACHE_TTL_MINUTES`) - Cache time-to-live in minutes (default: `5`)
- `cache.debugMode` (env: `CLOUD_OPS_CACHE_DEBUG_MODE`) - Enable cache debug logging (default: `false`)

### Provider Parameters
- `providers` (env: `CLOUD_OPS_PROVIDERS`) - Comma-separated list of providers to enable: `azure`, `gcp`, `aws` (default: all enabled)

## Performance Tuning

### Cache Configuration

The driver includes intelligent caching to improve performance for repeated queries:

```java
// Long-running applications: Use longer cache TTL
String url = "jdbc:cloudops:cache.ttlMinutes=60;cache.enabled=true";

// Development/debugging: Enable cache debug mode
String url = "jdbc:cloudops:cache.debugMode=true";

// High-frequency queries: Disable caching if data must be real-time
String url = "jdbc:cloudops:cache.enabled=false";
```

### Provider Selection

Limit providers to only what you need for better performance:

```java
// Only query Azure resources
String url = "jdbc:cloudops:providers=azure;azure.tenantId=xxx";

// Only query AWS and GCP
String url = "jdbc:cloudops:providers=aws,gcp";
```

### Best Practices

1. **Use appropriate cache TTL**: Set `cache.ttlMinutes` based on how fresh your data needs to be
2. **Limit provider scope**: Only enable providers you actually need
3. **Filter early**: Use WHERE clauses to filter at the cloud API level when possible
4. **Batch queries**: Group related queries to benefit from cache warming
5. **Monitor performance**: Enable `cache.debugMode=true` during development

## Error Handling

The driver provides detailed error messages for configuration issues:

```java
// Missing required configuration
try {
    Connection conn = DriverManager.getConnection("jdbc:cloudops:");
    // Will throw: "At least one cloud provider must be configured"
} catch (SQLException e) {
    System.err.println("Configuration error: " + e.getMessage());
}

// Invalid parameter values
// Will log warning: "Invalid integer value 'abc' for cache.ttlMinutes, using default: 5"
String url = "jdbc:cloudops:cache.ttlMinutes=abc";
```

## Troubleshooting

### Common Issues

1. **"At least one cloud provider must be configured"**
   - Ensure you provide valid credentials for Azure, GCP, or AWS
   - Check environment variables are properly set

2. **Authentication errors**
   - Verify cloud provider credentials are correct and have appropriate permissions
   - For Azure: Check tenant ID, client ID, and client secret
   - For GCP: Verify service account JSON file path and permissions
   - For AWS: Confirm access keys and region settings

3. **Empty result sets**
   - Check that your credentials have read permissions for the resources you're querying
   - Verify the subscription IDs, project IDs, or account IDs are correct

4. **Performance issues**
   - Enable cache debug mode: `cache.debugMode=true`
   - Adjust cache TTL: `cache.ttlMinutes=30`
   - Limit providers: `providers=azure` (only what you need)

### Debug Mode

Enable debug logging to troubleshoot issues:

```bash
export CLOUD_OPS_CACHE_DEBUG_MODE="true"
```

Or via connection string:
```java
String url = "jdbc:cloudops:cache.debugMode=true";
```

## Maven Dependency

```xml
<dependency>
    <groupId>com.kenstott.components</groupId>
    <artifactId>cloud-ops-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

## License

Apache License 2.0