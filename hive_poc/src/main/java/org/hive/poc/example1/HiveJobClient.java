package org.hive.poc.example1;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

public class HiveJobClient {


    public String submit(final String user,final String hiveQuery,final String hiveScriptFilePath,final String hiveCustomFunctionLibs,final String callbackURL,final ApplicationConfiguration config) 
            throws IOException, InterruptedException{


        UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());

        final HiveJob job = new HiveJob();

        int jobRunStatus = ugi.doAs(
                new PrivilegedExceptionAction<Integer>() {
                    public Integer run() throws Exception {

                        Configuration conf = new Configuration();
                        conf.set(ApplicationConfiguration.HIVE_ARCHIVE_PATH, config.get(ApplicationConfiguration.HIVE_ARCHIVE_PATH));
                        conf.set(ApplicationConfiguration.HIVE_EXEC_PATH, config.get(ApplicationConfiguration.HIVE_EXEC_PATH));
                        conf.set(ApplicationConfiguration.HIVE_CONFIG_FILE, config.get(ApplicationConfiguration.HIVE_CONFIG_FILE));
                        conf.set(ApplicationConfiguration.JOB_OUTPUT_PATH, config.get(ApplicationConfiguration.JOB_OUTPUT_PATH));
                        conf.set("user.name", user);

                        String[] appArgs = new String[]{
                                "-e",hiveQuery,
                                "-f",hiveScriptFilePath,
                                "-hive_aux_jars",hiveCustomFunctionLibs,
                                "callbackURL",callbackURL
                         };

                        int jobRunStatus = -1;
                        jobRunStatus = ToolRunner.run(conf,job, appArgs);
                        return jobRunStatus;
                    }
                }
         );


        if(jobRunStatus!=0) {
            System.out.println("Hive job Failed");
        }else{
            System.out.println("Hive job success jobID:"+job.getSubmittedJobID());
        }

        return job.getSubmittedJobID();
    }

    public static void main(String[] args) {
        try {
            new HiveJobClient().submit("abc",null,"hdfs:///apps/templeton/hive.sql",null,"",new ApplicationConfiguration());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
