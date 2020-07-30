package com.xyy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

import java.security.PrivilegedAction;

public class HdfsKerDemo {
    public static void main(String[] args) throws IOException {


        System.setProperty("java.security.krb5.conf", "C:\\prod_krb5.conf");
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication" , "kerberos");
        UserGroupInformation. setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("hive/node03-bigdata-prod-bj1.ybm100.top@YBM100.COM", "C:\\prod_hive.keytab");

        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        System.out.println(loginUser);

        FileSystem fs = null;
        loginUser.doAs(new PrivilegedAction<Object>(){
            public Object run() {
                Path path = new Path("/user/HTTP/test.txt");
                Configuration conf = new Configuration();
                FileSystem fs = null;
                boolean exists;
                try {
                    fs = FileSystem.get(conf);
                    exists = fs.exists(path);
                    System.out.print(fs.isFile(path));
                    System.out.println("这个文件判定的结果是：" + exists);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return null;
            }
        });

    }
}
