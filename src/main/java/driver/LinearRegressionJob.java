package driver;

import mr.LinearRegressionMapper;
import mr.ShuffleMapper;
import mr.ShuffleReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.Utils;

/**
 * 第二步：使用随机梯度下降（SGD）求解每个mapper的最优函数
 * 此方法只针对只有一个全局最优解的情况（如一元一次线性回归）
 * 因为如果每个mapper得到不同的参数Theta，那么在Reducer端如何整合就会有不同的方式
 * Created by fanzhe on 2016/10/23.
 */
public class LinearRegressionJob extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration() , new LinearRegressionJob(),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        if(args.length!= 4){

            System.err.println("Usage : driver.LinearRegressionJob <input> <output> <theta0;theta1;alpha> <splitter>");
            System.exit(-1);
        }
        Configuration conf = getConf();
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize",700L);
        String[] args2 = args[2].split(";");
        float theta0 = 1.0f;
        float theta1 = 0.0f;
        float alpha = 0.01f ;
        try{
            theta0 = Float.parseFloat(args2[0]);
            theta1 = Float.parseFloat(args2[1]);
            alpha =  Float.parseFloat(args2[2]);
        }catch (NumberFormatException e){
            e.printStackTrace();
        }
        conf.setFloat(Utils.LINEAR_THETA0,theta0);
        conf.setFloat(Utils.LINEAR_THETA1, theta1);
        conf.setFloat(Utils.LINEAR_ALPHA, alpha);
        conf.set(Utils.LINEAR_SPLITTER,args[3]);

        Job job = Job.getInstance(conf);

        job.setMapperClass(LinearRegressionMapper.class);
//        job.setReducerClass(LinearRegressionReducer.class);// 不使用mapper即可
        job.setNumReduceTasks(0);

//        job.setMapOutputKeyClass(FloatWritable.class);
//        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, Utils.str2Path(args[0]));
        FileOutputFormat.setOutputPath(job, Utils.str2Path(args[1]));
        Utils.delete(conf, args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
