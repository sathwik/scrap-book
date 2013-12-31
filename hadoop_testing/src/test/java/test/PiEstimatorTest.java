package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.examples.PiEstimator;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PiEstimatorTest {

    MapDriver<LongWritable, LongWritable, BooleanWritable,
    LongWritable> mapDriver;
    ReduceDriver<BooleanWritable, LongWritable, WritableComparable<?>,
    Writable> reduceDriver;
    MapReduceDriver<LongWritable, LongWritable, BooleanWritable,
    LongWritable, WritableComparable<?>, Writable> mapReduceDriver;

    @Before
    public void setUp() {
        PiEstimator.PiMapper mapper = new PiEstimator.PiMapper();
        PiEstimator.PiReducer reducer = new PiEstimator.PiReducer();
        mapDriver = new MapDriver<LongWritable, LongWritable,
        BooleanWritable, LongWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<BooleanWritable, LongWritable,
        WritableComparable<?>, Writable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, LongWritable,
        BooleanWritable,LongWritable,
        WritableComparable<?>, Writable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(10), new LongWritable(10));
        mapDriver.withOutput(new BooleanWritable(true), new LongWritable(10));
        mapDriver.addOutput(new BooleanWritable(false), new LongWritable(0));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testReducer() {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(10));
        reduceDriver.withInput(new BooleanWritable(true), values);

        try {
            reduceDriver.runTest();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
