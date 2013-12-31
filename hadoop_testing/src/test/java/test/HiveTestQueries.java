package test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;


public class HiveTestQueries implements Runnable{
    ExecutorService pool = null;
    boolean listen = false;
    BlockingQueue<Callable> queue  = null;
    Lock lock = new ReentrantLock();
    Condition cond = lock.newCondition();

    HiveTestQueries(){
        pool = Executors.newFixedThreadPool(10, new SimpleThreadFactory());
        queue = new LinkedBlockingQueue<Callable>();
    }

    class SimpleThreadFactory implements ThreadFactory {
        int threadCount = 0;

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("poolThread-"+threadCount++);
            return t;
        }
    }


    public static class HiveCallable implements Callable {

        public Object call() throws Exception {
            if(SessionState.get() != null){
                System.out.println(" Thread: "+Thread.currentThread() +" Name:"+Thread.currentThread().getName()+" Old SessionState: "+SessionState.get());
            }

            CliSessionState ss = null;
            Path parent = new Path(new File(".").getAbsolutePath());
            HiveConf conf = new HiveConf(SessionState.class);
            conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,
                    (new Path(parent,
                              "target/build/ql/test/data/warehouse/")).toString());
            try{
                ss = new CliSessionState(conf);

                File qf = new File(parent.toUri().getPath(), "target");
                File outf = new File(qf.getAbsolutePath());
                outf = new File(outf, qf.getName().concat(".out"));
                FileOutputStream fo = new FileOutputStream(outf);
                ss.out = new PrintStream(fo, true, "UTF-8");
                ss.err = new CachingPrintStream(fo, true, "UTF-8");
                ss.setIsSilent(true);

                SessionState.start(ss);

                if(SessionState.get() != null){
                    System.out.println(" Thread: "+Thread.currentThread() +" Name:"+Thread.currentThread().getName()+ " New SessionState: "+SessionState.get());
                }

                //String command = "CREATE EXTERNAL TABLE cas ( grms_cid                          STRING, cm15                              BIGINT, cm11                              STRING, se10                              BIGINT, postal_zip_cd                     STRING, se_zip_cd                         STRING, ww_industry_cd                    STRING, amount                            DOUBLE, trans_dt                          STRING, time_of_day_in                    STRING, mag_stripe_cd                     INT, appr_deny_cd                      INT, se_prev_visit_ind                 INT, cas_pkey                          BIGINT, product_typ                       STRING, currency_cd                       STRING, se_keyed_pct                      DOUBLE, int_cust_email_addr               STRING, cust_integer_ip_addr              BIGINT, int_cust_ip_addr                  STRING, lseq_nbr                          BIGINT, arm_proc_ind                      STRING, se_typ                            STRING, cid_create_dt                     STRING, super_phone_ind                   INT, crt_cd                            STRING, nis_org_id                        SMALLINT, nis_ind                           TINYINT, country_cd                        STRING, grms_product_cd                   STRING, prod_literal                      STRING, credit_loss_prob                  STRING, orig_mcc_cd                       INT, pos_card_pres_ind                 STRING, se_2_cm_avg_dist                  STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '${hiveconf:cas.location}'";
                String command = "use default";

                int success = 0;
                String tokens[] = command.split("\\s+");
                CommandProcessor proc = CommandProcessorFactory.get(tokens[0], (HiveConf) conf);
                if (proc instanceof Driver){
                    Driver drv = (Driver) proc;
                    if(success != drv.compile(command)){
                        //fail
                        System.out.println("fail");
                    }else{
                        System.out.println("success");
                    }
                }else{
                    CommandProcessorResponse response =  proc.run(command);
                    if(response.getResponseCode() != 0){
                        System.out.println("fail");
                    }else{
                        System.out.println("success");
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                ss.close();
                CommandProcessorFactory.clean(conf);
            }

            return null;
        }
    }

    public void run(){
        if(listen) return;

        listen = true;
        System.out.println("Started..");

        while(listen){
            lock.lock();
            Callable r = null;

            try {

                while((r = queue.poll()) !=  null)
                    pool.submit(r);
                System.out.println("Inside run(), about to wait for signal");
                cond.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally{
                lock.unlock();
            }
        }

    }
    public void start(){
        new Thread(this).start();
    }

    public void stop(){
        System.out.println("Stop called");
        listen = false;

        signal();

        if (pool != null){
            pool.shutdown();
        }
        System.out.println("Stopped");
    }

    public void submit(Callable r){
        System.out.println("Task submitted");
        try {
            queue.put(r);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void signal(){
        lock.lock();
        try {
            cond.signal();
        } finally{
            lock.unlock();
        }
    }

    public static class WorkerThread extends Thread{
        HiveTestQueries test = null;

        WorkerThread(HiveTestQueries test){
            this.test=test;
        }

        public void run(){
            test.submit(new HiveCallable());
        }

        public void get(){
            if(SessionState.get() != null){
                System.out.println(" Thread: "+Thread.currentThread() +" Name:"+Thread.currentThread().getName()+" Old SessionState: "+SessionState.get());
            }else{
                System.out.println(" Session State is null");
            }
        }
    }

    public static void sleepCurrentThread(long time){
        try {
            Thread.currentThread().sleep(time);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HiveTestQueries test = new HiveTestQueries();
        test.start();

        //HiveTestQueries.sleepCurrentThread(1000);

        for(int i=0;i<50;i++){
            test.submit(new HiveCallable());
        }

        test.signal();

        test.stop();
    }

}
