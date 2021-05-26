/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics;
import com.amazonaws.services.kinesisanalytics.operators.JsonToTimestreamPayloadFn;
import com.amazonaws.services.kinesisanalytics.operators.JsonToResultPayloadFn;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestream.TimestreamSink;
import com.amazonaws.services.timestream.TimestreamSinkOneHour;
import com.amazonaws.services.timestream.TimestreamSinkSixHour;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import main.java.com.amazonaws.services.timestream.TimestreamInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.WindowGroupedTable;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import com.amazonaws.services.kinesisanalytics.AggregationRecord;
import org.slf4j.Logger;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.slf4j.LoggerFactory;
import java.sql.Timestamp;
import org.apache.flink.table.api.Tumble;
import java.util.Properties;
import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

/**
 {
 "MeasureName": "cpu_system",
 "MeasureValue": "0.6222817133325357",
 "MeasureValueType": "DOUBLE",
 "Time": "1605044983",
 "TimeUnit": "SECONDS",
 "region": "us_east_1",
 "cell": "us_east_1-cell-1",
 "silo": "us_east_1-cell-1-silo-1",
 "availability_zone": "us_east_1-1",
 "microservice_name": "apollo",
 "instance_type": "r5.4xlarge",
 "os_version": "AL2",
 "instance_name": "i-zaZswmJk-apollo-0000.amazonaws.com"}

 {
    'MeasureName': datatype_name,
    'MeasureValue': str(row.value),
    'MeasureValueType': 'DOUBLE',
    'Time': str(row.ts),
    'TimeUnit': 'SECONDS',
    'port_num': str(row.port_num),
    'sub_sensor_index': str(row.sub_sensor_index),
    'sensor_id': str(row.sensor_id)
}
 */


public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	private static final String DEFAULT_STREAM_NAME = "TimestreamTestStream";
	private static final String DEFAULT_REGION_NAME = "us-west-2";

	public static DataStream<ObjectNode> createKinesisObjectSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {
	    //set Kinesis consumer properties
		Properties kinesisConsumerConfig = new Properties();
		//set the region the Kinesis stream is located in
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
				parameter.get("Region", DEFAULT_REGION_NAME));
		//obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

		if(adaptiveReadSettingStr.equals("true")) {
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
		} else {
			//poll new events from the Kinesis stream once every second
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
					parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
			// max records to get in shot
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
					parameter.get("SHARD_GETRECORDS_MAX", "10000"));
		}

		//create Kinesis source
		DataStream<ObjectNode> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
				//read events from the Kinesis stream passed in as a parameter
				parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
				//deserialize events with EventSchema
				new JsonNodeDeserializationSchema(),
				//using the previously defined properties
				kinesisConsumerConfig
		)).name("KinesisSource");

		return kinesisStream;
	}

	public static DataStream<String> createKinesisSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

		//set Kinesis consumer properties
		Properties kinesisConsumerConfig = new Properties();
		//set the region the Kinesis stream is located in
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
				parameter.get("Region", DEFAULT_REGION_NAME));
		//obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

		if(adaptiveReadSettingStr.equals("true")) {
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
		} else {
			//poll new events from the Kinesis stream once every second
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
					parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
			// max records to get in shot
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
					parameter.get("SHARD_GETRECORDS_MAX", "10000"));
		}

		//create Kinesis source
		DataStream<String> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
				//read events from the Kinesis stream passed in as a parameter
				parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
				//deserialize events with EventSchema
				new SimpleStringSchema(),
				//using the previously defined properties
				kinesisConsumerConfig
		)).name("KinesisSource");

		return kinesisStream;
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);
		LOG.info("MICHAEL START OF MAIN");

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> input = createKinesisSource(env, parameter);
		DataStream<ObjectNode> jsonInput = createKinesisObjectSource(env, parameter);

		//DataStream<Record> recordStream = jsonInput
        //        .map((ObjectNode object) -> {
        //            ObjectMapper mapper = new ObjectMapper();
        //            LOG.info("MICHAEL Mapped Record: " + object.toString());
        //            Record record = mapper.readValue(object.toString(), Record.class);
        //            return record;
        //        });

        //DataStream<Record> recordStreamWithTime = recordStream
        //        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Record>() {
        //            @Override
        //            public long extractAscendingTimestamp(Record element) {
        //                return element.timestamp.getTime();
        //            }});

		DataStream<TimestreamPoint> mappedInput =
				input.map(new JsonToTimestreamPayloadFn()).name("MaptoTimestreamPayload");

		DataStream<AggregationRecord> mappedInput2 =
				input.map(new JsonToResultPayloadFn()).name("MaptoResultPayload");

		DataStream<AggregationRecord> mappedInput3 =
				input.map(new JsonToResultPayloadFn()).name("MaptoResultPayloadSix");
		LOG.info("MICHAEL Defined Inputs");

        //Table recordTable = tableEnv.fromDataStream(recordStream, "MeasureName, MeasureValue, MeasureValueType, Time, timestamp.rowtime, TimeUnit, port_num, sub_sensor_index, sensor_id, sn");
        //tableEnv.registerTable("Records", recordTable);

        //Table record_table = tableEnv.scan("Records");

        //Table records1 = record_table.select("MeasureName, MeasureValue, MeasureValueType, Time, timestamp, TimeUnit, port_num, sub_sensor_index, sensor_id, sn");
        //WindowedTable records2 = record_table.window(Tumble.over("1.hour").on("timestamp").as("hourWindow"));
        //WindowGroupedTable records3 = records2.groupBy("hourWindow, sn");
        //Table results = records3.select("hourWindow.end as timestamp, MeasureValue.sum as valueSum, MeasureValue.avg as valueAvg, sn");
        //Table records = records3.select("MeasureName.end, MeasureValue.end, MeasureValueType.end, hourWindow.end as hour, Time.start, TimeUnit.end, port_num.end, sub_sensor_index.end, sensor_id.end, sn");
        //Table records = records3.select("hourWindow.end as hour, sn");

        //Table records = record_table
        //                    .select("MeasureName, MeasureValue, MeasureValueType, Time, timestamp, TimeUnit, port_num, sub_sensor_index, sensor_id, sn, eventtime.rowtime")
        //                    .window(Tumble.over("1.hour").on("eventtime").as("hourWindow"))
        //                    .groupBy("hourWindow, sn")
        //                    //.select("MeasureName, MeasureValue, MeasureValueType, hourWindow.end as hour, Time, TimeUnit, port_num, sub_sensor_index, sensor_id, sn");
        //                    .select("MeasureName, MeasureValue, MeasureValueType, Time, TimeUnit, port_num, sub_sensor_index, sensor_id, sn");

        //Table records = tableEnv.sqlQuery("SELECT COUNT(MeasureName) FROM Records GROUP BY sn");
        //Table records = tableEnv.sqlQuery("SELECT * FROM Records");
        //Table records = tableEnv.sqlQuery("SELECT MeasureName, MeasureValue, MeasureValue, Time, TimeUnit, port_num, sub_sensor_index, sensor_id FROM Records GROUP BY sn");

        //DataStream<Result> resultSet = tableEnv.toAppendStream(results, Result.class);

        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-west-2");

        //FlinkKinesisProducer<Result> streamSink = new FlinkKinesisProducer<Result>(new SerializationSchema<Result>() {

        //    @Override
        //    public byte[] serialize(Result element) {

        //        ObjectMapper mapper = new ObjectMapper();
        //        byte[] output;
        //        try {
        //            output = mapper.writeValueAsString(element).getBytes();
        //            //For debugging output
        //            LOG.info("MICHAEL DB Record: " + mapper.writeValueAsString(element));
        //        } catch (Exception e) {
        //            LOG.info("MICHAEL DB Record: Exception " + e);
        //            output = "".getBytes();
        //        }
        //        return output;
        //    }
        //}, outputProperties);

		String region = parameter.get("Region", "us-west-2").toString();
		String databaseName = parameter.get("TimestreamDbName", "kdaflink").toString();
		String tableName = parameter.get("TimestreamTableName", "kinesisdata2").toString();
		int batchSize = Integer.parseInt(parameter.get("TimestreamIngestBatchSize", "50"));

		TimestreamInitializer timestreamInitializer = new TimestreamInitializer(region);
		timestreamInitializer.createDatabase(databaseName);
		timestreamInitializer.createTable(databaseName, tableName);
		LOG.info("MICHAEL Created Tables/Databases");

		//SinkFunction<TimestreamPoint> sink = new TimestreamSink(region, "ZentraReadingsDB", "non-aggregate", batchSize);
		SinkFunction<TimestreamPoint> sink = new TimestreamSink(region, "kdaflink", "kinesisdata2", batchSize);
		//SinkFunction<AggregationRecord> streamSink = new TimestreamSinkOneHour(region, "ZentraReadingsDB", "aggregate", batchSize);
		SinkFunction<AggregationRecord> streamSink = new TimestreamSinkOneHour(region, "kdaflink", "kinesis_aggregation_data", batchSize);
		//SinkFunction<AggregationRecord> streamSinkSix = new TimestreamSinkSixHour(region, "ZentraReadingsDB", "aggregate", batchSize);
		SinkFunction<AggregationRecord> streamSinkSix = new TimestreamSinkSixHour(region, "kdaflink", "kinesis_aggregation_data", batchSize);
		mappedInput.addSink(sink);

		//resultSet.addSink(streamSink);
		mappedInput2.addSink(streamSink);
		mappedInput3.addSink(streamSinkSix);
		LOG.info("MICHAEL Added Sinks");

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
/***/

