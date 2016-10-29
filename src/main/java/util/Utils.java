package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.jar.JarOutputStream;

/**
 * Created by fanzhe on 2016/10/23.
 */
public class Utils {


    public static final String SHUFFLE_RANDN= "shuffle.randn";

    public static final String LINEAR_THETA0 = "linearregression.theta0";
    public static final String LINEAR_THETA1 = "linearregression.theta1";
    public static final String LINEAR_ALPHA = "linearregression.alpha";
    public static final String LINEAR_SPLITTER = "linearregression.splitter";

    public static final String SINGLE_LINEAR_PATH ="single.regression.path";

    public static final String MAPPER_OUTPUT_PREFIX = "part-";

    public static final String SINGLE_REDUCER_METHOD="single.reducer.method" ;

    public static Path str2Path(String pathStr){
        return new Path(pathStr);
    }

    public static boolean delete(Configuration conf,String path){
        try {
            FileSystem.get(conf).delete(str2Path(path), true);
        }catch (IOException e){
            return false;
        }
        return true;
    }

    /**
     * 从一个theta文件中读取theta 参数
     * @param path
     * @return
     */
    public static float[] readFromOneTheatFile(Configuration conf,Path path,String splitter) throws IOException {
        FSDataInputStream is = FileSystem.get(conf).open(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        String[] data = bufferedReader.readLine().split(splitter);
        bufferedReader.close();
        is.close();

        return str2float(data);
    }

    public static float[] str2float(String[] data){
        float[] theta = new float[data.length];
        for(int i = 0 ; i< data.length; i++){
            theta[i] = Float.parseFloat(data[i]);
        }
        return theta;
    }

    public static String floatArr2Str(float[] data,String splitter){
        StringBuffer buffer = new StringBuffer();
        for(int i=0;i< data.length-1;i++){
            buffer.append(data[i]).append(splitter);
        }
        buffer.append(data[data.length-1]);
        return buffer.toString();
    }
    private static Configuration conf ;
    public static Configuration getConf(){
        if(conf == null){
            conf = new Configuration();
            conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
            conf.set("fs.defaultFS", "hdfs://master:8020");// 指定namenode
            conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
            conf.set("yarn.resourcemanager.address","master:8032"); // 指定resourcemanager
            conf.set("yarn.resourcemanager.scheduler.address", "master:8030");// 指定资源分配器
            conf.set("mapreduce.jobhistory.address","master:10020");
//            conf.set("yarn.app.mapreduce.am.resource.mb","1024");
            conf.set("mapreduce.map.memory.mb","1024");
            conf.set("mapreduce.reduce.memory.mb","1024");
            conf.set("mapreduce.job.jar",JarUtil.jar(Utils.class));// 自动打包

//            conf.setLong("mapreduce.input.fileinputformat.split.maxsize",1024L);

        }
        return conf;
    }

    public static void main(String[] args){
        String path = JarUtil.jar(Utils.class);
        System.out.println(path);
    }

}
