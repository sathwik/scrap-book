package test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

    final Map<Object, Object> test = new HashMap<Object, Object>();
    final AtomicInteger counter = new AtomicInteger(0);
    private MiniDFSCluster dfsCluster = null;
    private MiniMRCluster mrCluster = null;

    private final Path input = new Path("input");
    private final Path output = new Path("output");
    private final Path target_dir = new Path((new File(".")).getAbsolutePath(),"target");
    private final Path tmp_dir = new Path(target_dir,"tmp");
    private final Path mapred_dir = new Path(tmp_dir,"mapred");
    private final Path mapred_local_dir = new Path(mapred_dir,"local");
    private final Path mapred_system_dir = new Path(mapred_dir,"system");
    private final Path hadoop_log_dir = new Path(tmp_dir,"log");

    @Before
    public void setUp() throws Exception {
        System.setProperty("hadoop.log.dir", hadoop_log_dir.toString());
        System.setProperty("test.build.data",tmp_dir.toString());

        Configuration conf = new Configuration();

        //conf.set("hadoop.tmp.dir",tmp_dir.toString());
        //conf.set("dfs.data.dir",dfs_data_dir.toString());
        //conf.set("dfs.name.dir",dfs_name_dir.toString());
        conf.set("mapred.local.dir",mapred_local_dir.toString());
        conf.set("mapred.system.dir",mapred_system_dir.toString());

        //dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfsCluster = new MiniDFSCluster(0,conf,1,true,true,null,null);
        dfsCluster.getFileSystem().makeQualified(input);
        dfsCluster.getFileSystem().makeQualified(output);

        assertNotNull("Cluster has a file system", dfsCluster.getFileSystem());
/*        mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri()
                .toString(), 1);*/
        JobConf jobConf = new JobConf(conf);
        mrCluster = new MiniMRCluster(0, 0, 1, dfsCluster.getFileSystem().getUri().toString(), 1, null, null, null, jobConf);
    }

    protected FileSystem getFileSystem() throws IOException {
        return dfsCluster.getFileSystem();
    }

    private void createInput() throws IOException {
        Writer wr = new OutputStreamWriter(getFileSystem().create(
                new Path(input, "wordcount")));
        wr.write("neeraj chaplot neeraj\n");
        wr.close();
    }

    @Test
    public void testJob() throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = mrCluster.createJobConf();

        createInput();

        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

        final String COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";
        Counters ctrs = job.getCounters();
        System.out.println("Counters: " + ctrs);
        long combineIn = ctrs.findCounter(COUNTER_GROUP,
                "COMBINE_INPUT_RECORDS").getValue();
        long combineOut = ctrs.findCounter(COUNTER_GROUP,
                "COMBINE_OUTPUT_RECORDS").getValue();
        long reduceIn = ctrs.findCounter(COUNTER_GROUP, "REDUCE_INPUT_RECORDS")
                .getValue();
        long mapOut = ctrs.findCounter(COUNTER_GROUP, "MAP_OUTPUT_RECORDS")
                .getValue();
        long reduceOut = ctrs.findCounter(COUNTER_GROUP,
                "REDUCE_OUTPUT_RECORDS").getValue();
        long reduceGrps = ctrs
                .findCounter(COUNTER_GROUP, "REDUCE_INPUT_GROUPS").getValue();

        assertEquals("map out = combine in", mapOut, combineIn);
        assertEquals("combine out = reduce in", combineOut, reduceIn);
        assertTrue("combine in > combine out", combineIn > combineOut);
        assertEquals("reduce groups = reduce out", reduceGrps, reduceOut);

        InputStream is = getFileSystem().open(new Path(output, "part-r-00000"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        assertEquals("chaplot\t1", reader.readLine());
        assertEquals("neeraj\t2", reader.readLine());
        assertNull(reader.readLine());
        reader.close();
    }

    @After
    public void tearDown() throws Exception {
        if (mrCluster != null) {
            System.out.println("Shutting down MRCluster");
            mrCluster.shutdown();
        }
        if (dfsCluster != null) {
            FileSystem dfs = dfsCluster.getFileSystem();
            dfs.delete(new Path("/input"), true);
            dfs.delete(new Path("/output"), true);
            dfsCluster.shutdown();
        }

    }
}
