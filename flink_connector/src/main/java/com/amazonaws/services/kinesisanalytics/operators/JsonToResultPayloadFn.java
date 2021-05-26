package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.kinesisanalytics.AggregationRecord;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class JsonToResultPayloadFn extends RichMapFunction<String, AggregationRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToTimestreamPayloadFn.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public AggregationRecord map(String jsonString) throws Exception {
        HashMap<String,String> map = new Gson().fromJson(jsonString,
                new TypeToken<HashMap<String, String>>(){}.getType());
        AggregationRecord dataPoint = new AggregationRecord();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // assuming these fields are present in every JSON record
            switch (key.toLowerCase()) {
                case "measurename":
                    dataPoint.setMeasureName(value);
                    break;
                case "measurevaluetype":
                    dataPoint.setMeasureValueType(value);
                    break;
                case "time":
                    dataPoint.setTime(Integer.parseInt(value));
                    break;
                case "timeunit":
                    dataPoint.setTimeUnit(value);
                    break;
                case "port_num":
                    dataPoint.setport_num(Integer.parseInt(value));
                    break;
                case "sub_sensor_index":
                    dataPoint.setsub_sensor_index(Integer.parseInt(value));
                    break;
                case "sensor_id":
                    dataPoint.setsensor_id(Integer.parseInt(value));
                    break;
                case "sn":
                    dataPoint.setsn(value);
                    break;
                case "measurevalue":
                    dataPoint.setMeasureValue(Double.parseDouble(value));
                    break;
                case "errorcode":
                    dataPoint.setErrorCode(Integer.parseInt(value));
                    break;
            }
        }
        LOG.info("MICHAEL " + jsonString + " was inserted and converted to AggregationRecord");

        return dataPoint;
    }
}