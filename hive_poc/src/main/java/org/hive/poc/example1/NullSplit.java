package org.hive.poc.example1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class NullSplit extends InputSplit implements Writable {

    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    public void readFields(DataInput arg0) throws IOException {}

    public void write(DataOutput arg0) throws IOException {}
}
