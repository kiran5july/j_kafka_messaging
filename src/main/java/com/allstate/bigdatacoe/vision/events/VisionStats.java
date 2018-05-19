package com.allstate.bigdatacoe.vision.events;

public class VisionStats implements java.io.Serializable{
    public int endpoint_id;
    public double mean;
    public double stddev;
    public long create_timestamp;

    public String toString(){
        return endpoint_id+","+mean+","+stddev;
    }
}
