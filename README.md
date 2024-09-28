# Spark DynamoDB Connector
This is a V2 data source for Apache Spark to interact with Amazon DynamoDB. 
This work is inspired in large parts by [Spark DynamoDB](https://github.com/audienceproject/spark-dynamodb) 
which is no longer maintained.

This project is actively being developed and the code may have some bugs. 
If you find a bug, please open an issue. Contributions are always welcome :).

## Features
- Dynamic and paginated scans with PartiQL support for column and filter push down.
- Throughput rate control based on target fraction of provisioned table/index capacity.
- Support for automatic schema discovery and case classes (when using Scala).
- Support for Scala 2.12, 2.13 and Spark 3+ with AWS SDK v2.
- JDK 11, 17 and 21+ are supported. This is subject to change based on Spark's JVM support.

## How to build
The library requires selection of specific Scala version for build in `build.sbt`.
Spark 3.5 version is used for builds and any version above 3.3 should work as the data source API
has major changes then.
- Change the section below as desired
```scala
lazy val scala212 = "2.12.20"
lazy val scala213 = "2.13.15"

// Update the line below.
scalaVersion := scala212

```
- Run the assembly command
```shell
export JAVA_HOME=<Path of your JDK>
sbt clean test assembly
```

## Sample usage
### Scala
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.github.drnushooz.spark.dynamodb.implicits._
import com.github.drnushooz.spark.dynamodb.attribute

// Case class for sample employee
// Attribute in Dynamodb shall have an underscore
case class Employee(name: String, division: String, @attribute("tenure_months") tenureMonths: Int)

val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

// Create a dataframe from Dynamodb table
val dynamoDf = spark.read.dynamodb("employees") // <-- DataFrame of Row objects with inferred schema.

// Scan the table for the first 10 items (the order is arbitrary) and print them.
dynamoDf.show(10)

// Find average tenure per division
val employeesDs = spark.read.dynamodbAs[Employee]("employees")
val avgTenureDs = employeesDs.agg($"division", avg($"tenureMonths"))

// Write to a destination table 
avgTenureDs.write.dynamodb("avg_tenure_division")
```

### Python
```python
# Load a DataFrame from a Dynamo table. Only incurs the cost of a single scan for schema inference.
dynamoDf = spark.read.option("tableName", "employees1").format("dynamodb").load()

# Scan the table for the first 10 items and print them.
dynamoDf.show(10)

# Write to a destination table
dynamoDf.write.option("tableName", "employees2").format("dynamodb").save()
```

## Configuration
### Reader and writer level properties to load and save

| Name      | Purpose                                                                 | Default value |
|-----------|-------------------------------------------------------------------------|---------------|
| `region`  | Region of the table to read from or write to                            | `us-east-1`   |
| `roleArn` | Role to assume if the account is different from the current environment |               |

### Spark reader level parameters


| Name                      | Purpose                                                                              | Default value                     |
|---------------------------|--------------------------------------------------------------------------------------|-----------------------------------|
| `readPartitions`          | Number of partitions to split the initial RDD when loading the data into Spark.      | `maxPartitionBytes` size chunks   |
| `maxPartitionBytes`       | Maximum size of each partition in Bytes                                              | `134217728` (128 MiB)             |
| `defaultParallelism`      | Number of parallel reads from DynamoDB                                               | `sparkContext.defaultParallelism` |
| `targetCapacity`          | Fraction of provisioned read capacity on the table (or index) to consume for reading | `1` (100%)                        |
| `stronglyConsistentReads` | Whether to use strongly consistent reads                                             | `false`                           |
| `bytesPerRCU`             | Number of bytes which can be read per second per read capacity unit                  | `4096` (4 KiB)                    |
| `filterPushdown`          | Enable filter pushdown for scan requests                                             | `true`                            |
| `throughput`              | Desired percentage throughput to use for on-demand tables                            | `100`                             |

### Spark writer level parameters
| Name             | Purpose                                                                                | Default value |
|------------------|----------------------------------------------------------------------------------------|---------------|
| `writeBatchSize` | Number of items to send per batch write call                                           | `25`          |
| `targetCapacity` | Fraction of provisioned write capacity on the table to consume for writing or updating | `1` (100%)    |
| `update`         | Use UpdateItem requests instead of batch writes                                        | `false`       |
| `throughput`     | fraction of provisioned write capacity on the table (or index) to consume for reading  | `100`         |
| `inferSchema`    | Infer schema for writes. Useful while writing complex tables                           | `false`       |

### System properties

| Name                    | Purpose                                                                              | Default value                    |
|-------------------------|--------------------------------------------------------------------------------------|----------------------------------|
| `aws.profile`           | IAM profile to use for default credentials provider                                  | `us-east-1`                      |
| `aws.dynamodb.region`   | Region for all AWS API calls                                                         | `134217728` (128 MiB)            |
| `aws.dynamodb.endpoint` | Endpoint to use for DynamoDB APIs                                                    | `sparkContext.defaultParallelism` |
| `aws.sts.endpoint`      | fraction of provisioned read capacity on the table (or index) to consume for reading | `1` (100%)                         |
