package test;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class TestUGI {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            conf.set("hadoop.job.ugi", "sathwik,hadoop");
            conf.set("user.name", "sathwik");

            UserGroupInformation someUser = UserGroupInformation.login(conf);
            System.out.println(someUser.getUserName());
            String[] groups = someUser.getGroupNames();
            for (int i = 0; i < groups.length; i++) {
                System.out.println(groups[i]);
            }

        } catch (LoginException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
