package org.hive.poc.example1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

public class HiveJob extends Configured implements Tool {

    private JobID jobID = null;

    public String getSubmittedJobID(){
        return (jobID != null) ? jobID.toString() : null;
    }

    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();

        Job hiveJob = Job.getInstance(conf);

        hiveJob.addCacheArchive(new URI(conf.get(ApplicationConfiguration.HIVE_ARCHIVE_PATH)));
        hiveJob.addCacheFile(new URI(conf.get(ApplicationConfiguration.HIVE_CONFIG_FILE)));

        setArgsToJob(arg0, hiveJob);

        hiveJob.setJobName(getClass().getSimpleName());
        hiveJob.setJarByClass(HiveMapper.class);
        hiveJob.setMapperClass(HiveMapper.class);
        hiveJob.setMapOutputKeyClass(NullWritable.class);
        hiveJob.setMapOutputValueClass(NullWritable.class);
        hiveJob.setInputFormatClass(NullInputFormat.class);

        NullOutputFormat<NullWritable,NullWritable> outFormat = new NullOutputFormat<NullWritable,NullWritable>();
        hiveJob.setOutputFormatClass(outFormat.getClass());
        hiveJob.setNumReduceTasks(0);

        hiveJob.submit();
        jobID = hiveJob.getJobID();
        return 0;
    }

    private void setArgsToJob(String[] args,Job job) throws URISyntaxException, IOException{
        for (int i = 0; i < args.length; i++) {
            switch (args[i]){

            case "-e":
                    if(args[i+1] != null && args[i+1].trim().length()>0){
                        job.getConfiguration().set("-e", args[i+1]);
                    }
                    i++;
                    break;

                case "-f": 
                    String file = args[i+1];
                    if(file != null && file.trim().length()>0){
                        job.getConfiguration().set("-f", args[i+1]);
                        job.addCacheFile(new URI(args[i+1]));
                    }
                    i++;
                    break;

                case "-hive_aux_jars":
                    String aux_jars = args[i+1];
                    if(aux_jars != null && aux_jars.trim().length()>0){
                        String[] files = aux_jars.split(",");
                        for (int j = 0; j < files.length; j++) {
                            job.addFileToClassPath(new Path(files[j]));
                        }
                    }
                    i++;
                    break;

                case "-callbackURL":
                    String callbackURL = args[i+1];
                    if(callbackURL != null && callbackURL.trim().length()>0)
                        job.getConfiguration().set("job.end.notification.url", callbackURL);
                    i++;
                    break;
                default: 
                    break;
            }
        }
    }
}
