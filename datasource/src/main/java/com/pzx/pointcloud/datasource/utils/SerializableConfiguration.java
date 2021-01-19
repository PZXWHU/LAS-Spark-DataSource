package com.pzx.pointcloud.datasource.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.util.Utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * copy from org.apache.spark.util.Configuration
 */
public class SerializableConfiguration implements Serializable {

    public transient Configuration configuration;

    public SerializableConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        configuration.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException{
        configuration = new Configuration(false);
        configuration.readFields(in);
    }



}
