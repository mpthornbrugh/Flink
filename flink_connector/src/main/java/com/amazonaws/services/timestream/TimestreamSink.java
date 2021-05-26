package com.amazonaws.services.timestream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.*;
import com.amazonaws.services.timestreamwrite.model.Record;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;

// references:
// 1. https://stackoverflow.com/questions/58742213/buffering-transformed-messagesexample-1000-count-using-apache-flink-stream-pr
// 2. https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html
/**
 * Sink function for Flink to ingest data to Timestream
 */
public class TimestreamSink extends RichSinkFunction<TimestreamPoint> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamSink.class);

    private static final long RECORDS_FLUSH_INTERVAL_MILLISECONDS = 60L * 1000L; // One minute

    private transient ListState<Record> checkpointedState;

    private transient AmazonTimestreamWrite writeClient;

    private final String region;
    private final String db;
    private final String table;
    private final Integer batchSize;

    private List<Record> bufferedRecords;
    private long emptyListTimetamp;
    private Map record_buffer;
    //private Map hour_aggregate_buffer;
    //private Map six_hour_aggregate_buffer;

    public TimestreamSink(String region, String databaseName, String tableName, int batchSize) {
        this.region = region;
        this.db = databaseName;
        this.table = tableName;
        this.batchSize = batchSize;
        this.bufferedRecords = new ArrayList<>();
        //this.hour_aggregate_buffer = new HashMap<>();
        //this.six_hour_aggregate_buffer = new HashMap<>();
        this.record_buffer = new HashMap();
        this.emptyListTimetamp = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        this.writeClient = AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion(this.region)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    @Override
    public void invoke(TimestreamPoint value, Context context) throws Exception {
        List<Dimension> dimensions = new ArrayList<>();

        for(Map.Entry<String, String> entry : value.getDimensions().entrySet()) {
            Dimension dim = new Dimension().withName(entry.getKey()).withValue(entry.getValue());
            dimensions.add(dim);
        }
        String sn = value.getSN();
        Dimension sn_dim = new Dimension().withName("sn").withValue(sn);
        dimensions.add(sn_dim);

        Record measure = new Record()
                .withDimensions(dimensions)
                .withMeasureName(value.getMeasureName())
                .withMeasureValueType(value.getMeasureValueType())
                .withMeasureValue(value.getMeasureValue())
                .withTimeUnit(value.getTimeUnit())
                .withTime(String.valueOf(value.getTime()));

        // "{Dimensions: [{Name: availability_zone,Value: us_east_1-1,}, {Name: microservice_name,Value: demeter,}, {Name: instance_name,Value: i-zaZswmJk-demeter-0000.amazonaws.com,}, {Name: os_version,Value: AL2012,}, {Name: cell,Value: us_east_1-cell-1,}, {Name: silo,Value: us_east_1-cell-1-silo-3,}, {Name: region,Value: us_east_1,}, {Name: instance_type,Value: c5.16xlarge,}],MeasureName: file_descriptors_in_use,MeasureValue: 42.37244359144161,MeasureValueType: DOUBLE,Time: 1605041178,TimeUnit: SECONDS}"

        bufferedRecords.add(measure);
        LOG.info("MICHAEL Inside TimestreamSink");
        //List existing_one_hour = hour_aggregate_buffer.getOrDefault(sn, new ArrayList<>());
        //List existing_six_hour = six_hour_aggregate_buffer.getOrDefault(sn, new ArrayList<>());
        //existing_one_hour.add(measure);
        //existing_six_hour.add(measure);
        //hour_aggregate_buffer.put(sn, existing_one_hour);
        //six_hour_aggregate_buffer.put(sn, existing_six_hour);

        if(shouldPublish()) {
            //Map one_hour_records_for_agg = oneHourRecordsForAgg();
            //Map six_hour_records_for_agg = sixHourRecordsForAgg();
            WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                    .withDatabaseName(this.db)
                    .withTableName(this.table)
                    .withRecords(bufferedRecords);

            try {
                WriteRecordsResult writeRecordsResult = this.writeClient.writeRecords(writeRecordsRequest);
                LOG.debug("writeRecords Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
                bufferedRecords.clear();
                emptyListTimetamp = System.currentTimeMillis();

                // (1) Define the AWS Region in which the function is to be invoked
                Regions region = Regions.fromName("us-west-2");
                // (2) Instantiate AWSLambdaClientBuilder to build the Lambda client
                AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard()
                                                    .withRegion(region);
                // (3) Build the client, which will ultimately invoke the function
                AWSLambda client = builder.build();
                // (4) Create an InvokeRequest with required parameters
                InvokeRequest req = new InvokeRequest()
                                           .withFunctionName("zentra_alerter")
                                           .withPayload("{}");
                // (5) Invoke the function and capture response
                InvokeResult result = client.invoke(req);
                LOG.info("MICHAEL Lambda Result: " + result);
            } catch (Exception e) {
                LOG.error("Error: " + e);
            }
        }
    }

    // Method to validate if record batch should be published.
    // This method would return true if the accumulated records has reached the batch size.
    // Or if records have been accumulated for last RECORDS_FLUSH_INTERVAL_MILLISECONDS time interval.
    private boolean shouldPublish() {
        if (bufferedRecords.size() == batchSize) {
            LOG.debug("Batch of size " + bufferedRecords.size() + " should get published");
            return true;
        } else if(System.currentTimeMillis() - emptyListTimetamp >= RECORDS_FLUSH_INTERVAL_MILLISECONDS) {
            LOG.debug("Records after flush interval should get published");
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Record element : bufferedRecords) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Record> descriptor =
                new ListStateDescriptor<>("recordList",
                        Record.class);

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (functionInitializationContext.isRestored()) {
            for (Record element : checkpointedState.get()) {
                bufferedRecords.add(element);
            }
        }
    }
}