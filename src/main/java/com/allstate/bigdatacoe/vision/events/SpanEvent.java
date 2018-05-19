package com.allstate.bigdatacoe.vision.events;

public class SpanEvent  implements java.io.Serializable{
    public int endpoint_id;
    public long application_id;
    public int host_id;
    public int domain_id;
    public String method;
    public int duration;
    public int status_code;
    public boolean error_occurred;
    public long span_created_at;
}
