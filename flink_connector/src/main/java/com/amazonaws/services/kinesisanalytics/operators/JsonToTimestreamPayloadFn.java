package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.timestream.TimestreamPoint;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class JsonToTimestreamPayloadFn extends RichMapFunction<String, TimestreamPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToTimestreamPayloadFn.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public TimestreamPoint map(String jsonString) throws Exception {
        //LOG.info("MICHAEL JSON: " + jsonString);
        HashMap<String,String> map = new Gson().fromJson(jsonString,
                new TypeToken<HashMap<String, String>>(){}.getType());
        TimestreamPoint dataPoint = new TimestreamPoint();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // assuming these fields are present in every JSON record
            switch (key.toLowerCase()) {
                case "timestamp":
                    dataPoint.setTime(Long.parseLong(value));
                    break;
                case "timeunit":
                    dataPoint.setTimeUnit(value);
                    break;
                case "measurename":
                    dataPoint.setMeasureName(value);
                    break;
                case "measurevalue":
                    dataPoint.setMeasureValue(value);
                    break;
                case "measurevaluetype":
                    dataPoint.setMeasureValueType(value);
                    break;
                case "sn":
                    dataPoint.setSN(value);
                    break;
                default:
                    dataPoint.addDimension(key, value);
            }
        }
        //String combined_key = map.get("sn") + "|" + map.get("port_num") + "|" + map.get("sub_sensor_index") + "|" + map.get("sensor_id");
        //dataPoint.addDimension("reading_metadata_key", combined_key);
        dataPoint.setPointJson(jsonString);
        LOG.info("MICHAEL " + jsonString + " was inserted and converted to TimestreamPoint(" + dataPoint.getSN() + ", " + dataPoint.getMeasureName() + ", " + dataPoint.getMeasureValue() + ", " + dataPoint.getTime() );

        return dataPoint;
    }
}
