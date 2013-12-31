package org.hive.poc.example1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ApplicationConfiguration {
    public static String HIVE_ARCHIVE_PATH = "myapp.hive.archive.path";
    public static String HIVE_EXEC_PATH = "myapp.hive.exec.path";
    public static String HIVE_CONFIG_FILE = "myapp.hive.config.file";
    public static String JOB_OUTPUT_PATH = "myapp.job.ouput.path";

    private static String HIVE_ARCHIVE_PATH_DEFAULT_VALUE = "hdfs:///apps/templeton/hive-0.12.0.tar.gz";
    private static String HIVE_EXEC_PATH_DEFAULT_VALUE = "hive-0.12.0.tar.gz/hive-0.12.0/bin/hive";
    private static String HIVE_CONFIG_FILE_DEFAULT_VALUE = "hdfs:///apps/templeton/hive-site.xml";
    private static String JOB_OUTPUT_PATH_DEFAULT_VALUE = "/user/root/hivejob";

    private Map<String,String> paramMap = new HashMap<String,String>();


    public ApplicationConfiguration(){
        Properties prop = new Properties();
        try {
            prop.load(ApplicationConfiguration.class.getResourceAsStream("/org/hive/poc/example1/configuration.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        paramMap.put(HIVE_ARCHIVE_PATH,prop.getProperty(HIVE_ARCHIVE_PATH,HIVE_ARCHIVE_PATH_DEFAULT_VALUE));
        paramMap.put(HIVE_EXEC_PATH,prop.getProperty(HIVE_EXEC_PATH,HIVE_EXEC_PATH_DEFAULT_VALUE));
        paramMap.put(HIVE_CONFIG_FILE,prop.getProperty(HIVE_CONFIG_FILE,HIVE_CONFIG_FILE_DEFAULT_VALUE));
        paramMap.put(JOB_OUTPUT_PATH,prop.getProperty(JOB_OUTPUT_PATH,JOB_OUTPUT_PATH_DEFAULT_VALUE));
    }

    public String get(String key){
        return paramMap.get(key);
    }

    public static void main(String[] args){
        ApplicationConfiguration config = new ApplicationConfiguration();
        System.out.println(config.get(HIVE_ARCHIVE_PATH));
    }
}
