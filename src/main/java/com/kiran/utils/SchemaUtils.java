package com.kiran.utils;

import org.apache.avro.Schema;

public final class SchemaUtils {

    public static org.apache.avro.Schema getStatsSchema() {

        org.apache.avro.Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"VisionStats\",\"namespace\":\"com.allstate.bigdatacoe.vision.events\",\"fields\":[{\"name\":\"endpoint_id\",\"type\":\"int\"},{\"name\":\"mean\",\"type\":\"double\",\"default\":0.0},{\"name\":\"stddev\",\"type\":\"double\",\"default\":0.0},{\"name\":\"create_timestamp\",\"type\":\"long\",\"default\":0,\"logicalType\":\"timestamp-millis\"}]}"

        );
        return schema;
    }

    public void stringTest(){
        String sch = "{\"namespace\": \"kiran.events\", " +
                "\"type\": \"record\", " +
                " \"name\": \"KiranEventStats\", " +
                " \"fields\": [ " +
                "     {\"name\": \"endpoint_id\", \"type\": \"int\"}, " +
                "     {\"name\": \"mean\", \"type\": \"double\"}, " +
                "     {\"name\": \"stddev\", \"type\": \"double\"}, " +
                "     {\"name\": \"create_timestamp\", \"type\": \"long\"} " +
                " ] " +
                "} ";
    }

}

