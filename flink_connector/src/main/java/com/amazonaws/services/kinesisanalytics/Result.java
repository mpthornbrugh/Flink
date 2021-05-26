package com.amazonaws.services.kinesisanalytics;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class Result {
    public long timestamp;
    public double valueAvg;
    public double valueSum;
    public String sn;
    public String measureName;
    public String port_num;
    public String sub_sensor_index;
    public String sensor_id;
    public String error_code;
    public String MeasureValueType;
    public Integer SampleSize;
    public Double lttb;

    public Result() {
    }

    public Result(long timestamp, double valueAvg, double valueSum, String sn, String measureName, String port_num, String sub_sensor_index, String sensor_id, String error_code, String MeasureValueType, Integer SampleSize, Double lttb)
    {
        this.timestamp = timestamp;
        this.valueAvg = valueAvg;
        this.valueSum = valueSum;
        this.sn = sn;
        this.measureName = measureName;
        this.port_num = port_num;
        this.sub_sensor_index = sub_sensor_index;
        this.sensor_id = sensor_id;
        this.error_code = error_code;
        this.MeasureValueType = MeasureValueType;
        this.SampleSize = SampleSize;
        this.lttb = lttb;
    }

    public Integer getSampleSize()
    {
        return SampleSize;
    }

    public void setSampleSize(Integer SampleSize)
    {
        this.SampleSize = SampleSize;
    }

    public String getMeasureValueType()
    {
        return MeasureValueType;
    }

    public String getMeasureName()
    {
        return measureName;
    }

    public String getPortNum()
    {
        return port_num;
    }

    public String getSubSensorIndex()
    {
        return sub_sensor_index;
    }

    public String getSensorID()
    {
        return sensor_id;
    }

    public String getErrorCode()
    {
        return error_code;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public double getValueAvg()
    {
        return valueAvg;
    }

    public void setValueAvg(double valueAvg)
    {
        this.valueAvg = valueAvg;
    }

    public double getValueSum()
    {
        return valueSum;
    }

    public void setValueSum(double valueSum)
    {
        this.valueSum = valueSum;
    }

    public String getSN()
    {
        return sn;
    }

    public void setSN(String sn)
    {
        this.sn = sn;
    }

    public Double getLttb()
    {
        return lttb;
    }

    public void setLttb(double lttb)
    {
        this.lttb = lttb;
    }
}