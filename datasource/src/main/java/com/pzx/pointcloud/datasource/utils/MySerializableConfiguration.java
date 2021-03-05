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
public class MySerializableConfiguration implements Serializable {

    public transient Configuration configuration;

    public MySerializableConfiguration() { }

    public MySerializableConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

        private void writeObject(ObjectOutputStream out) throws IOException {
        System.out.println("序列化");
        out.defaultWriteObject();
        configuration.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException{
        System.out.println("反序列化");
        configuration = new Configuration(false);
        configuration.readFields(in);
    }



}
