package com.amazonaws.services.kinesisanalytics;
import java.sql.Timestamp;

public class AggregationRecord {
    public String MeasureName;
    public double MeasureValue;
    public String MeasureValueType;
    public Integer Time;
    public String TimeUnit;
    public Integer port_num;
    public Integer sub_sensor_index;
    public Integer sensor_id;
    public String sn;
    public Integer ErrorCode;

    public AggregationRecord() {}

    public AggregationRecord(String MeasureName, double MeasureValue, String MeasureValueType, Integer Time, String TimeUnit, Integer port_num, Integer sub_sensor_index, Integer sensor_id, String sn, Integer ErrorCode)
    {
        this.MeasureName = MeasureName;
        this.MeasureValue = MeasureValue;
        this.MeasureValueType = MeasureValueType;
        this.Time = Time;
        this.TimeUnit = TimeUnit;
        this.port_num = port_num;
        this.sub_sensor_index = sub_sensor_index;
        this.sensor_id = sensor_id;
        this.sn = sn;
        this.ErrorCode = ErrorCode;
    }

    //MeasureName
    public String getMeasureName()
    {
        return MeasureName;
    }

    public void setMeasureName(String MeasureName)
    {
        this.MeasureName = MeasureName;
    }

    //MeasureValue
    public double getMeasureValue()
    {
        return MeasureValue;
    }

    public void setMeasureValue(double MeasureValue)
    {
        this.MeasureValue = MeasureValue;
    }

    //MeasureValueType
    public String getMeasureValueType()
    {
        return MeasureValueType;
    }

    public void setMeasureValueType(String MeasureValueType)
    {
        this.MeasureValueType = MeasureValueType;
    }

    //Time
    public Integer getTime()
    {
        return Time;
    }

    public void setTime(Integer Time)
    {
        this.Time = Time;
    }

    //TimeUnit
    public String getTimeUnit()
    {
        return TimeUnit;
    }

    public void setTimeUnit(String TimeUnit)
    {
        this.TimeUnit = TimeUnit;
    }

    //port_num
    public Integer getport_num()
    {
        return port_num;
    }

    public void setport_num(Integer port_num)
    {
        this.port_num = port_num;
    }

    //sub_sensor_index Integer
    public Integer getsub_sensor_index()
    {
        return sub_sensor_index;
    }

    public void setsub_sensor_index(Integer sub_sensor_index)
    {
        this.sub_sensor_index = sub_sensor_index;
    }

    //sensor_id Integer
    public Integer getsensor_id()
    {
        return sensor_id;
    }

    public void setsensor_id(Integer sensor_id)
    {
        this.sensor_id = sensor_id;
    }

    //sn String
    public String getsn()
    {
        return sn;
    }

    public void setsn(String sn)
    {
        this.sn = sn;
    }

    //ErrorCode Integer
    public Integer getErrorCode()
    {
        return ErrorCode;
    }

    public void setErrorCode(Integer ErrorCode)
    {
        this.ErrorCode = ErrorCode;
    }
}