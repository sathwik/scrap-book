package org.hive.poc.example1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class HiveMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {


    public void run(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String jobID = context.getJobID().toString();

        List<String> commands = new ArrayList<String>();

        commands.add(conf.get("myapp.hive.exec.path"));
        commands.add("--config");
        commands.add(".");
        commands.add("--service");
        commands.add("cli");
        if(conf.get("-e") != null){
            commands.add("-e");
            commands.add(conf.get("-e"));
        }else if(conf.get("-f") !=null){
            commands.add("-f");
            Path scriptFile = new Path(conf.get("-f"));
            Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if(localCacheFiles != null){
                for (int i = 0; i < localCacheFiles.length; i++) {
                    if(scriptFile.getName().equalsIgnoreCase(localCacheFiles[i].getName())){
                        commands.add(localCacheFiles[i].toString());
                    }
                }
            }
        }

        Path jobOuputPath = createFilePaths(conf, jobID, conf.get("myapp.job.ouput.path"));

        ProcessBuilder builder = new ProcessBuilder(commands);
        builder.environment().put("HADOOP_PROXY_USER", conf.get("user.name"));
        final Process process = builder.start();

        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    public void run() {
                        process.destroy();
                    }
                }
         );

        CaptureOutputFromProcess errorIO = new CaptureOutputFromProcess(conf,jobID,process.getErrorStream(),jobOuputPath,true);
        Thread errThread = new Thread(errorIO);
        errThread.setDaemon(true);

        CaptureOutputFromProcess successIO = new CaptureOutputFromProcess(conf,jobID,process.getInputStream(),jobOuputPath,false);
        Thread sThread = new Thread(successIO);
        sThread.setDaemon(true);

        errThread.start();
        sThread.start();

        process.waitFor();

        errThread.join();
        sThread.join();
    }

    protected Path createFilePaths(Configuration conf,String jobID, String outputPath){
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            String jobOutputDir = outputPath +"/"+ jobID;
            Path fsPath = new Path(jobOutputDir);

            if(!fileSystem.exists(fsPath));
                fileSystem.mkdirs(fsPath);

            return fsPath;
        } catch (IOException e) {
        }
        return null;
    }

    public class CaptureOutputFromProcess implements Runnable {
        private InputStream io = null;
        private Path jobOuputPath = null;
        Configuration conf = null;
        String jobID = null;
        boolean errorStream = false;

        public CaptureOutputFromProcess(Configuration conf,String jobID, InputStream io,Path jobOuputPath, boolean errorStream){
            this.conf = conf;
            this.jobID = jobID;
            this.io = io;
            this.jobOuputPath = jobOuputPath;
            this.errorStream = errorStream;
        }

        public void run() {
            captureDataFromStream(io, jobOuputPath);
        }

        protected void captureDataFromStream(InputStream io,Path outputPath){
            FSDataOutputStream outputStream = null;
            FileSystem fileSystem = null;
            String line = null;
            BufferedReader reader = null;
            PrintWriter ptWriter = null;

            try {
                fileSystem = FileSystem.get(conf);
                Path outputFile = null;

                if(!errorStream){
                    outputFile = new Path(outputPath, "success");
                } else{
                    outputFile = new Path(outputPath, "error");
                }

                outputStream =  fileSystem.create(outputFile, true);
                reader = new BufferedReader(new InputStreamReader(io),512);
                ptWriter = new PrintWriter(outputStream,true);

                while((line = reader.readLine()) != null)
                    ptWriter.println(line);

            } catch (IOException e) {
                e.printStackTrace();
            }finally{

                if(ptWriter != null){
                    ptWriter.close();
                }

                if(reader != null){
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if(fileSystem!=null){
                    try {
                        fileSystem.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }

}
