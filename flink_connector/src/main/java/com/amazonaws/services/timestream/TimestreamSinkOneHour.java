package com.amazonaws.services.timestream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.*;
import com.amazonaws.services.kinesisanalytics.Result;
import com.amazonaws.services.kinesisanalytics.AggregationRecord;
import com.amazonaws.services.kinesisanalytics.BufferedRecord;
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
public class TimestreamSinkOneHour extends RichSinkFunction<AggregationRecord> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamSinkOneHour.class);

    private static final long RECORDS_FLUSH_INTERVAL_MILLISECONDS = 60L * 1000L; // One minute

    private transient ListState<AggregationRecord> checkpointedState;

    private transient AmazonTimestreamWrite writeClient;

    private final String region;
    private final String db;
    private final String table;
    private final Integer batchSize;

    private long emptyListTimetamp;
    private Map record_buffer;
    private HashMap<String, ArrayList<BufferedRecord>> bufferedRecords;
    private HashMap<String, Double> lastLTTB;
    private HashMap<String, ArrayList<ArrayList<BufferedRecord>>> lttbDataStorage;
    private List<Record> timestreamRecords;

    public TimestreamSinkOneHour(String region, String databaseName, String tableName, int batchSize) {
        this.region = region;
        this.db = databaseName;
        this.table = tableName;
        this.batchSize = batchSize;
        this.bufferedRecords = new HashMap<>();
        this.lastLTTB = new HashMap<>();
        this.lttbDataStorage = new HashMap<>();
        this.record_buffer = new HashMap();
        this.timestreamRecords = new ArrayList<>();
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
    public void invoke(AggregationRecord value, Context context) throws Exception {
        String key = value.getsn() + "|" + value.getport_num() + "|" + value.getsub_sensor_index() + "|" + value.getsensor_id();
        BufferedRecord new_record = new BufferedRecord(value);
        if (bufferedRecords.containsKey(key)) {
            bufferedRecords.get(key).add(new_record);
        }
        else {
            ArrayList<BufferedRecord> records = new ArrayList<BufferedRecord>();
            records.add(new_record);
            bufferedRecords.put(key, records);
        }
        LOG.info("MICHAEL Inside TimestreamSinkOneHour");
        ArrayList<Result> aggregate_records = new ArrayList<Result>();
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        writeClient = AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion(this.region)
                .withClientConfiguration(clientConfiguration)
                .build();

        for (Map.Entry mapElement : bufferedRecords.entrySet()) {
            String map_key = (String)mapElement.getKey();
            ArrayList<BufferedRecord> records = (ArrayList<BufferedRecord>)mapElement.getValue();
            HashMap<Integer, ArrayList<BufferedRecord>> agg_recs = new HashMap<>();
            ArrayList<Integer> agg_times = new ArrayList<>();
            ArrayList<ArrayList<BufferedRecord>> lttb_lists = new ArrayList<>();

            for (BufferedRecord element : records) {
                int hour_window = element.getHourWindow();
                if (agg_recs.containsKey(hour_window)) {
                    agg_recs.get(hour_window).add(element);
                }
                else {
                    ArrayList<BufferedRecord> buff_recs = new ArrayList<BufferedRecord>();
                    buff_recs.add(element);
                    agg_recs.put(hour_window, buff_recs);
                    agg_times.add(hour_window);
                }
            }

            if (agg_times.size() > 1) {
                for (int i = 0; i < agg_times.size() - 1; i++) {
                    boolean values_set = false;
                    String sn = "";
                    String measure_name = "";
                    String port_num = "";
                    String sub_sensor_index = "";
                    String sensor_id = "";
                    String error_code = "";
                    String measure_value_type = "";
                    Integer hour_start_ts = 0;
                    ArrayList<BufferedRecord> current_buff_recs = agg_recs.get(agg_times.get(i));
                    if (lttbDataStorage.containsKey(map_key)) {
                        lttb_lists = lttbDataStorage.get(map_key);
                    }
                    lttb_lists.add(current_buff_recs);
                    double sum = 0;
                    for (BufferedRecord current_rec : current_buff_recs) {
                        if (!values_set) {
                            sn = current_rec.getsn();
                            measure_name = current_rec.getMeasureName();
                            port_num = String.valueOf(current_rec.getport_num());
                            sub_sensor_index = String.valueOf(current_rec.getsub_sensor_index());
                            sensor_id = String.valueOf(current_rec.getsensor_id());
                            error_code = String.valueOf(current_rec.getErrorCode());
                            measure_value_type = current_rec.getMeasureValueType();
                            hour_start_ts = current_rec.getHourWindow() * 3600;
                            values_set = true;
                        }
                        sum += current_rec.getMeasureValue();
                        records.remove(current_rec);
                    }
                    double average = sum / current_buff_recs.size();
                    Double lttb = null;
                    Double last_lttb = null;
                    if (lttb_lists.size() > 1) {
                        if (lastLTTB.containsKey(map_key)) {
                            last_lttb = lastLTTB.get(map_key);
                        }
                        lttb = largestTriangleThreeBuckets(lttb_lists.get(0), lttb_lists.get(1), last_lttb);
                        lastLTTB.put(map_key, lttb);
                        lttb_lists.remove(0);
                    }
                    lttbDataStorage.put(map_key, lttb_lists);
                    Result aggregate = new Result((long)hour_start_ts, average, sum, sn, measure_name, port_num, sub_sensor_index, sensor_id, error_code, measure_value_type, current_buff_recs.size(), lttb);
                    aggregate_records.add(aggregate);
                }
                bufferedRecords.put(map_key, records);
            }
        }

        LOG.info("Have " + aggregate_records.size() + " records to send.");
        if (aggregate_records.size() > 0) {
            List<Dimension> sum_dimensions = new ArrayList<>();
            List<Dimension> avg_dimensions = new ArrayList<>();
            List<Dimension> lttb_dimensions = new ArrayList<>();

            for (Result agg_rec : aggregate_records) {
                sum_dimensions = new ArrayList<>();
                avg_dimensions = new ArrayList<>();
                lttb_dimensions = new ArrayList<>();

                //String combined_key = agg_rec.getSN() + "|" + agg_rec.getPortNum() + "|" + agg_rec.getSubSensorIndex()  + "|" + agg_rec.getSensorID();
                //Dimension reading_key_dim = new Dimension().withName("reading_metadata_key").withValue(combined_key);
                Dimension error_code_dim = new Dimension().withName("error_code").withValue(agg_rec.getErrorCode());
                Dimension time_window_dim = new Dimension().withName("time_window").withValue("1");
                //sum_dimensions.add(reading_key_dim);
                sum_dimensions.add(error_code_dim);
                sum_dimensions.add(time_window_dim);
                Dimension sum_dim = new Dimension().withName("agg_type").withValue("sum");
                sum_dimensions.add(sum_dim);

                //avg_dimensions.add(reading_key_dim);
                avg_dimensions.add(error_code_dim);
                avg_dimensions.add(time_window_dim);
                Dimension avg_dim = new Dimension().withName("agg_type").withValue("average");
                avg_dimensions.add(avg_dim);

                Record measure_sum = new Record()
                    .withDimensions(sum_dimensions)
                    .withMeasureName(agg_rec.getMeasureName())
                    .withMeasureValueType(agg_rec.getMeasureValueType())
                    .withMeasureValue(String.valueOf(agg_rec.getValueSum()))
                    .withTimeUnit("SECONDS")
                    .withTime(String.valueOf(agg_rec.getTimestamp()));

                LOG.info("Sending record to Timestream. SN: " + agg_rec.getSN() + ", value: " + agg_rec.getMeasureValueType() + ", ts: " + String.valueOf(agg_rec.getTimestamp()) + ", port_num: " + agg_rec.getPortNum() + ", sub: " + agg_rec.getSubSensorIndex() + ", sensor: " + agg_rec.getSensorID() + ", error: " + agg_rec.getErrorCode() );

                Record measure_avg = new Record()
                    .withDimensions(avg_dimensions)
                    .withMeasureName(agg_rec.getMeasureName())
                    .withMeasureValueType(agg_rec.getMeasureValueType())
                    .withMeasureValue(String.valueOf(agg_rec.getValueAvg()))
                    .withTimeUnit("SECONDS")
                    .withTime(String.valueOf(agg_rec.getTimestamp()));

                timestreamRecords.add(measure_sum);
                timestreamRecords.add(measure_avg);

                if (agg_rec.getLttb() != null) {
                    //lttb_dimensions.add(reading_key_dim);
                    lttb_dimensions.add(error_code_dim);
                    lttb_dimensions.add(time_window_dim);
                    Dimension lttb_dim = new Dimension().withName("agg_type").withValue("lttb");
                    lttb_dimensions.add(lttb_dim);

                    Record measure_lttb = new Record()
                        .withDimensions(lttb_dimensions)
                        .withMeasureName(agg_rec.getMeasureName())
                        .withMeasureValueType(agg_rec.getMeasureValueType())
                        .withMeasureValue(String.valueOf(agg_rec.getLttb()))
                        .withTimeUnit("SECONDS")
                        .withTime(String.valueOf(agg_rec.getTimestamp()));

                    timestreamRecords.add(measure_lttb);
                }

            }

            if (shouldPublish()) {
                //Send aggregate_records to lambda
                try {
                    WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                        .withDatabaseName("ZentraReadingsDB")
                        .withTableName("aggregate")
                        .withRecords(timestreamRecords);

                    WriteRecordsResult writeRecordsResult = writeClient.writeRecords(writeRecordsRequest);
                    LOG.info("MICHAEL Successfully Sent Records!");
                }
                catch(RejectedRecordsException e) {
                    LOG.info("MICHAEL Rejected Records Exception Error: " + e.getRejectedRecords() );
                }
                timestreamRecords.clear();
                emptyListTimetamp = System.currentTimeMillis();
            }
        }
    }

    private boolean shouldPublish() {
        if (timestreamRecords.size() == 100) {
            return true;
        } else if (System.currentTimeMillis() - emptyListTimetamp >= RECORDS_FLUSH_INTERVAL_MILLISECONDS) {
            return true;
        }
        return false;
    }

    private double largestTriangleThreeBuckets(ArrayList<BufferedRecord> current_bucket, ArrayList<BufferedRecord> next_bucket, Double previous_value) {
        if (previous_value == null) {
            previous_value = current_bucket.get(0).getMeasureValue();
        }
        double next_y_sum = 0.0;
        for (BufferedRecord record : next_bucket) {
            next_y_sum += (double)record.getMeasureValue();
        }
        double avg_y = next_y_sum/next_bucket.size();
        int best_index = 0;
        int current_index = 0;
        double max_area = 0.0;
        for (BufferedRecord rec : current_bucket) {
            double current_value = rec.getMeasureValue();
            double area = (1*(current_value-avg_y) + 2*(avg_y-previous_value) + 3*(previous_value-current_value))/2;
            if (area > max_area) {
                best_index = current_index;
            }
            current_index += 1;
        }
        return current_bucket.get(best_index).getMeasureValue();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Map.Entry mapElement : bufferedRecords.entrySet()) {
            ArrayList<AggregationRecord> records = (ArrayList<AggregationRecord>)mapElement.getValue();
            for (AggregationRecord element : records) {
                checkpointedState.add(element);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<AggregationRecord> descriptor =
                new ListStateDescriptor<>("recordList",
                        AggregationRecord.class);

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        //if (functionInitializationContext.isRestored()) {
        //    for (AggregationRecord element : checkpointedState.get()) {
        //        bufferedRecords.add(element);
        //    }
        //}
    }
}
