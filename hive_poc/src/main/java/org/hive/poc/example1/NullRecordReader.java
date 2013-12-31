package org.hive.poc.example1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NullRecordReader extends RecordReader<NullWritable, NullWritable> {

    public NullWritable getCurrentKey() throws IOException,
            InterruptedException {
        return NullWritable.get();
    }


    public NullWritable getCurrentValue() throws IOException,
            InterruptedException {
        return NullWritable.get();
    }


    public float getProgress() throws IOException, InterruptedException {
        return 0f;
    }


    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }


    public void close() throws IOException {
    }


    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
    }
}
