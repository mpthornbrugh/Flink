<!-- This sample application is part of the Timestream prerelease documentation. The prerelease documentation is confidential and is provided under the terms of your nondisclosure agreement with Amazon Web Services (AWS) or other agreement governing your receipt of AWS confidential information. -->

# Apache Flink sample data connector

Sample application that reads data from Flink and writes to Amazon Timestream

----
## How to test it

Java 1.8 is the recommended version for using Kinesis Data Analytics for Apache Flink Application. If you have multiple Java versions ensure to export Java 1.8 to your `JAVA_HOME` environment variable.

1. Ensure that you have [Apache Maven](https://maven.apache.org/install.html) installed. You can test your Apache Maven install with the following command:
   ```
   mvn -version
   ```
   
2. Run the build script.
   ```
   mvn package -Dflink.version=1.8.2 -U
   ```

3. Upload to S3