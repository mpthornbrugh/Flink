package com.amazonaws.services.kinesisanalytics;
import java.sql.Timestamp;
import com.amazonaws.services.kinesisanalytics.AggregationRecord;

public class BufferedRecord {
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
    public Integer HourWindow;
    public Integer SixHourWindow;
    public AggregationRecord base_record;

    public BufferedRecord() {}

    public BufferedRecord(AggregationRecord record)
    {
        this.MeasureName = record.getMeasureName();
        this.MeasureValue = record.getMeasureValue();
        this.MeasureValueType = record.getMeasureValueType();
        int timestamp = record.getTime();
        this.Time = timestamp;
        this.TimeUnit = record.getTimeUnit();
        this.port_num = record.getport_num();
        this.sub_sensor_index = record.getsub_sensor_index();
        this.sensor_id = record.getsensor_id();
        this.sn = record.getsn();
        this.ErrorCode = record.getErrorCode();
        int hour_window = timestamp/3600;
        int six_hour_window = timestamp/21600;
        this.HourWindow = hour_window;
        this.SixHourWindow = six_hour_window;
        this.base_record = record;
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

    //HourWindow Integer
    public Integer getHourWindow()
    {
        return HourWindow;
    }

    public void setHourWindow(Integer HourWindow)
    {
        this.HourWindow = HourWindow;
    }

    //SixHourWindow Integer
    public Integer getSixHourWindow()
    {
        return SixHourWindow;
    }

    public void setSixHourWindow(Integer SixHourWindow)
    {
        this.SixHourWindow = SixHourWindow;
    }
}