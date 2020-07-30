package com.xyy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        Configuration configuration = new Configuration();
        /*configuration.set("fs.defaultFS","hdfs://hadoop102:9000");
        FileSystem fs = FileSystem.get(configuration);
        fs.mkdirs(new Path("/user/hadoop/input1"));*/


        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"hadoop");
        fs.mkdirs(new Path("/user/hadoop/input2"));

        fs.close();


    }



    @Test
    public  void testCopyFromLocal() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("dfs.replication","1");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"hadoop");
        fs.copyFromLocalFile(new Path("D:\\hello.txt"),new Path("/user/hadoop/input/hello"));
        fs.close();
    }
    @Test
    public  void testCopyToLocal() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("dfs.replication","1");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"hadoop");
        fs.copyToLocalFile(new Path("/user/hadoop/input/hello"),new Path("D:\\hello2.txt"));
        fs.close();
    }

    @Test
    public void testRmdir() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"hadoop");
        fs.delete(new Path("/user"),true);

        fs.close();
    }

    @Test
    public void testRename() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"hadoop");

        //fs.rename(new Path("/user/hadoop/input/hello"),new Path("/user/hadoop/input/hello1"));
        //fs.rename(new Path("/user"),new Path("/user1"));
        fs.rename(new Path("/user1"),new Path("/user"));
        fs.close();
    }

    @Test
    public void testListFiles() throws  Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"hadoop");

        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("/"), true);

        while (it.hasNext()){
            System.out.println("------------");
            LocatedFileStatus fileStatus = it.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getAccessTime());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getReplication());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation blockLocation:blockLocations){
                String[] hosts = blockLocation.getHosts();
                for(String host: hosts){
                    System.out.println(host);
                }

                System.out.println(blockLocation.getLength());
                String[] names = blockLocation.getNames();
                for (String name :names){
                    System.out.println(name);
                }

            }

            System.out.println(fileStatus.getPermission());

        }


    }
    @Test
    public void testListStatus() throws Exception{

        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "hadoop");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testUpload() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "hadoop");

        FileInputStream in = new FileInputStream(new File("D:\\hadoop-2.7.2.tar.gz"));
        FSDataOutputStream out = fs.create(new Path("/hadoop-2.7.2.tar.gz"));
        IOUtils.copyBytes(in,out,configuration);
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);

        fs.close();

    }


    @Test
    public void testDownload() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "hadoop");

        FileOutputStream out = new FileOutputStream(new File("D:\\hello3.txt"));
        FSDataInputStream in = fs.open(new Path("/user.t"));
        IOUtils.copyBytes(in,out,configuration);
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);

        fs.close();

    }


    @Test
    public void testSeekFile1() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");

        FSDataInputStream in = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream out = new FileOutputStream("D:/hadoop-2.7.2.tar.gz.p1");

        byte[] buf = new byte[1024];
        for(int i=0; i<1024*128;i++){

            in.read(buf);
            out.write(buf);

        }
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);

        fs.close();

    }
    @Test
    public void testSeekFile2() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "hadoop");

        FSDataInputStream in = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream out = new FileOutputStream("D:/hadoop-2.7.2.tar.gz.p3");

        in.seek(1024 * 1024 * 128);

        IOUtils.copyBytes(in,out,conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);

        fs.close();

    }
}
