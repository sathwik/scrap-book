package org.hive.poc.example1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NullInputFormat extends InputFormat<NullWritable, NullWritable> {

    public List<InputSplit> getSplits(JobContext arg0) throws IOException,
            InterruptedException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        splits.add(new NullSplit());
        return splits;
    }

    public RecordReader<NullWritable, NullWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new NullRecordReader();
    }

}
